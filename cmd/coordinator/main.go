package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"Two-Phase-Commit/coordinator"
	"Two-Phase-Commit/participants"
	"Two-Phase-Commit/proto"
	"Two-Phase-Commit/protocol"
	"Two-Phase-Commit/wal"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/lipgloss"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"strings"
)

var (
	titleStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#FAFAFA")).
			Background(lipgloss.Color("#7D56F4")).
			Padding(0, 1)

	borderBox = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("#7D56F4")).
			Padding(0, 1).
			Width(70)

	statusOnlineStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#04B575")).
			Bold(true)

	statusOfflineStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF4672")).
			Bold(true)

	commitStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#04B575"))

	abortStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF4672"))

	infoStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#00D7D7"))

	helpStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#626262"))
)

type GRPCParticipant struct {
	id     string
	client proto.ParticipantServiceClient
}

func (g *GRPCParticipant) Prepare(tx protocol.Transaction) (protocol.MessageType, error) {
	req := &proto.PrepareRequest{
		TxId:    tx.ID,
		Payload: tx.Payload,
		Data:    tx.Data,
	}

	res, err := g.client.Prepare(context.Background(), req)
	if err != nil {
		return protocol.VOTE_NO, err
	}

	if res.Vote == proto.Vote_VOTE_YES {
		return protocol.VOTE_YES, nil
	}
	return protocol.VOTE_NO, fmt.Errorf("%s", res.Error)
}

func (g *GRPCParticipant) Commit(txID string) error {
	res, err := g.client.Commit(context.Background(), &proto.CommitRequest{TxId: txID})
	if err != nil {
		return err
	}
	if !res.Ok {
		return fmt.Errorf("commit failed: %s", res.Error)
	}
	return nil
}

func (g *GRPCParticipant) Abort(txID string) error {
	res, err := g.client.Abort(context.Background(), &proto.AbortRequest{TxId: txID})
	if err != nil {
		return err
	}
	if !res.Ok {
		return fmt.Errorf("abort failed: %s", res.Error)
	}
	return nil
}

func (g *GRPCParticipant) Get(key string) (string, bool) {
	res, err := g.client.Get(context.Background(), &proto.GetRequest{Key: key})
	if err != nil || !res.Found {
		return "", false
	}
	return res.Value, true
}

func (g *GRPCParticipant) GetAll() (map[string]string, error) {
	res, err := g.client.GetAll(context.Background(), &proto.GetAllRequest{})
	if err != nil {
		return nil, err
	}
	return res.Data, nil
}

func (g *GRPCParticipant) Ping() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := g.client.Get(ctx, &proto.GetRequest{Key: ""})
	return err == nil
}

type model struct {
	coord     *coordinator.Coordinator
	p1        *GRPCParticipant
	p2        *GRPCParticipant
	input     textinput.Model
	history   []string
	txCounter int
	p1Online  bool
	p2Online  bool
}

type tickMsg struct {
	p1Online bool
	p2Online bool
}

func checkHealth(p1, p2 *GRPCParticipant) tea.Cmd {
	return tea.Tick(800*time.Millisecond, func(t time.Time) tea.Msg {
		return tickMsg{
			p1Online: p1.Ping(),
			p2Online: p2.Ping(),
		}
	})
}

func initialModel(coord *coordinator.Coordinator, p1, p2 *GRPCParticipant) model {
	ti := textinput.New()
	ti.Placeholder = "SET key val, DEL key, GET key"
	ti.Focus()
	ti.CharLimit = 150
	ti.Width = 50
	ti.Prompt = "2pc-cluster > "

	return model{
		coord:    coord,
		p1:       p1,
		p2:       p2,
		input:    ti,
		history:  []string{"Connected to P1 (:50051) and P2 (:50052) via gRPC."},
		p1Online: p1.Ping(),
		p2Online: p2.Ping(),
	}
}

func (m model) Init() tea.Cmd {
	return tea.Batch(textinput.Blink, checkHealth(m.p1, m.p2))
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
	case tickMsg:
		m.p1Online = msg.p1Online
		m.p2Online = msg.p2Online
		return m, checkHealth(m.p1, m.p2)
	case tea.KeyMsg:
		switch msg.Type {
		case tea.KeyCtrlC, tea.KeyEsc:
			return m, tea.Quit

		case tea.KeyEnter:
			val := strings.TrimSpace(m.input.Value())
			if val == "" {
				return m, nil
			}

			if val == "exit" || val == "quit" {
				return m, tea.Quit
			}

			m.input.Reset()

			op, err := protocol.ParseCommand(val)
			if err != nil {
				m.history = append(m.history, fmt.Sprintf("ERR: %v", err))
				return m, nil
			}

			if op.Type == protocol.OpGet {
				res, ok := m.p1.Get(op.Key)
				if !ok {
					res, ok = m.p2.Get(op.Key)
				}
				if !ok {
					m.history = append(m.history, infoStyle.Render(fmt.Sprintf("GET %s -> (nil)", op.Key)))
				} else {
					m.history = append(m.history, commitStyle.Render(fmt.Sprintf("GET %s -> %q", op.Key, res)))
				}
				if len(m.history) > 8 {
					m.history = m.history[len(m.history)-8:]
				}
				return m, nil
			}

			if op.Type == protocol.OpKeys {
				data, err := m.p1.GetAll()
				if err != nil {
					data, err = m.p2.GetAll()
				}
				if err != nil {
					m.history = append(m.history, abortStyle.Render(fmt.Sprintf("ERR: %v", err)))
				} else if len(data) == 0 {
					m.history = append(m.history, infoStyle.Render("(empty database)"))
				} else {
					for k, v := range data {
						m.history = append(m.history, infoStyle.Render(fmt.Sprintf("  • %s: %s", k, v)))
					}
				}
				if len(m.history) > 8 {
					m.history = m.history[len(m.history)-8:]
				}
				return m, nil
			}

			m.txCounter++
			tx := protocol.Transaction{
				ID:      fmt.Sprintf("tx-%d", m.txCounter),
				Payload: val,
			}

			start := time.Now()
			err = m.coord.Begin(tx)
			dur := time.Since(start).Round(time.Millisecond)

			if err != nil {
				m.history = append(m.history, abortStyle.Render(fmt.Sprintf("[%s] ABORT: %s (%s) - %v", tx.ID, val, dur, err)))
			} else {
				m.history = append(m.history, commitStyle.Render(fmt.Sprintf("[%s] COMMIT: %s (%s)", tx.ID, val, dur)))
			}

			if len(m.history) > 8 {
				m.history = m.history[len(m.history)-8:]
			}

			return m, nil
		}
	}

	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m model) View() string {
	var b strings.Builder

	b.WriteString(titleStyle.Render("2PC DISTRIBUTED CLUSTER (gRPC)") + "\n\n")

	p1Status := statusOfflineStyle.Render("OFFLINE")
	if m.p1Online {
		p1Status = statusOnlineStyle.Render("ONLINE")
	}

	p2Status := statusOfflineStyle.Render("OFFLINE")
	if m.p2Online {
		p2Status = statusOnlineStyle.Render("ONLINE")
	}

	b.WriteString(fmt.Sprintf("Network Nodes: P1 [:50051 %s] | P2 [:50052 %s]\n\n",
		p1Status,
		p2Status,
	))

	b.WriteString("Activity Feed:\n")
	for _, h := range m.history {
		b.WriteString("  " + h + "\n")
	}

	b.WriteString("\n" + m.input.View() + "\n\n")
	b.WriteString(helpStyle.Render("Commands: SET <k> <v> • DEL <k> • GET <k> • KEYS • Esc to exit"))

	return borderBox.Render(b.String()) + "\n"
}

func main() {
	conn1, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to P1: %v", err)
	}
	defer conn1.Close()
	p1 := &GRPCParticipant{id: "P1", client: proto.NewParticipantServiceClient(conn1)}

	conn2, err := grpc.NewClient("localhost:50052", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("failed to connect to P2: %v", err)
	}
	defer conn2.Close()
	p2 := &GRPCParticipant{id: "P2", client: proto.NewParticipantServiceClient(conn2)}

	coordWAL, err := wal.NewWAL("logs/coordinator.wal")
	if err != nil {
		log.Fatalf("failed to create coord WAL: %v", err)
	}
	defer coordWAL.Close()

	parts := []participants.Participant{p1, p2}
	coord := coordinator.NewCoordinator(coordWAL, parts, 2*time.Second)
	_ = coord.Recover()

	p := tea.NewProgram(initialModel(coord, p1, p2))
	if _, err := p.Run(); err != nil {
		log.Fatalf("error running TUI: %v", err)
	}
}
