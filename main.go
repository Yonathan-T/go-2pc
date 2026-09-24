package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"Two-Phase-Commit/coordinator"
	"Two-Phase-Commit/participants"
	"Two-Phase-Commit/protocol"
	"Two-Phase-Commit/wal"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
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
			Width(65)

	statusStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#04B575")).
			Bold(true)

	commitStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#04B575"))

	abortStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#FF4672"))

	infoStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#3C3C3C"))

	helpStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#626262"))
)

type model struct {
	coord     *coordinator.Coordinator
	part1     *participants.Node
	part2     *participants.Node
	input     textinput.Model
	history   []string
	txCounter int
}

func initialModel(coord *coordinator.Coordinator, p1, p2 *participants.Node) model {
	ti := textinput.New()
	ti.Placeholder = "SET key val, GET key, DEL key"
	ti.Focus()
	ti.CharLimit = 150
	ti.Width = 50
	ti.Prompt = "2pc-db > "

	return model{
		coord:   coord,
		part1:   p1,
		part2:   p2,
		input:   ti,
		history: []string{"Database cluster initialized. Ready for transactions."},
	}
}

func (m model) Init() tea.Cmd {
	return textinput.Blink
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmd tea.Cmd

	switch msg := msg.(type) {
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
				m.addHistory(abortStyle.Render(fmt.Sprintf("ERR: %v", err)))
				return m, nil
			}

			if op.Type == protocol.OpGet {
				res, ok := m.part1.GetData(op.Key)
				if !ok {
					m.addHistory(infoStyle.Render(fmt.Sprintf("GET %s -> (nil)", op.Key)))
				} else {
					m.addHistory(commitStyle.Render(fmt.Sprintf("GET %s -> %q", op.Key, res)))
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
				m.addHistory(abortStyle.Render(fmt.Sprintf("[%s] ABORT: %s (%s)", tx.ID, val, dur)))
			} else {
				m.addHistory(commitStyle.Render(fmt.Sprintf("[%s] COMMIT: %s (%s)", tx.ID, val, dur)))
			}

			return m, nil
		}
	}

	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m *model) addHistory(entry string) {
	m.history = append(m.history, entry)
	if len(m.history) > 8 {
		m.history = m.history[len(m.history)-8:]
	}
}

func (m model) View() string {
	var b strings.Builder

	b.WriteString(titleStyle.Render("TWO-PHASE COMMIT DISTRIBUTED DATABASE") + "\n\n")

	b.WriteString(fmt.Sprintf("Cluster: Coord [%s] | P1 [%s] | P2 [%s]\n\n",
		statusStyle.Render("ONLINE"),
		statusStyle.Render("ONLINE"),
		statusStyle.Render("ONLINE"),
	))

	b.WriteString("Activity Feed:\n")
	for _, h := range m.history {
		b.WriteString("  " + h + "\n")
	}

	b.WriteString("\n" + m.input.View() + "\n\n")
	b.WriteString(helpStyle.Render("Commands: SET <k> <v> • GET <k> • DEL <k> • Esc to exit"))

	return borderBox.Render(b.String()) + "\n"
}

func main() {
	if err := os.MkdirAll("logs", 0755); err != nil {
		log.Fatalf("failed to create logs directory: %v", err)
	}

	coordWAL, err := wal.NewWAL("logs/coordinator.wal")
	if err != nil {
		log.Fatalf("failed to create coord WAL: %v", err)
	}
	defer coordWAL.Close()

	p1WAL, err := wal.NewWAL("logs/participant_1.wal")
	if err != nil {
		log.Fatalf("failed to create p1 WAL: %v", err)
	}
	defer p1WAL.Close()

	p2WAL, err := wal.NewWAL("logs/participant_2.wal")
	if err != nil {
		log.Fatalf("failed to create p2 WAL: %v", err)
	}
	defer p2WAL.Close()

	part1 := participants.NewNode("P1", p1WAL)
	part2 := participants.NewNode("P2", p2WAL)
	parts := []participants.Participant{part1, part2}

	coord := coordinator.NewCoordinator(coordWAL, parts, 2*time.Second)

	_ = part1.Recover()
	_ = part2.Recover()
	_ = coord.Recover()

	p := tea.NewProgram(initialModel(coord, part1, part2))
	if _, err := p.Run(); err != nil {
		log.Fatalf("error running TUI: %v", err)
	}
}
