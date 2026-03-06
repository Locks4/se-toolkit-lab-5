import { useState, useEffect } from 'react'
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
} from 'chart.js'
import { Bar, Line } from 'react-chartjs-2'

ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
)

// API Response Types
interface ScoreBucket {
  bucket: string
  count: number
}

interface TimelineEntry {
  date: string
  submissions: number
}

interface PassRateEntry {
  task: string
  avg_score: number
  attempts: number
}

interface LabItem {
  id: number
  type: string
  title: string
  parent_id: number | null
  description: string
  attributes: Record<string, unknown>
  created_at: string
}

// Chart data types for react-chartjs-2
interface ChartData {
  labels: string[]
  datasets: {
    label: string
    data: number[]
    backgroundColor?: string | string[]
    borderColor?: string | string[]
    fill?: boolean
    tension?: number
  }[]
}

const STORAGE_KEY = 'api_key'

function Dashboard() {
  const [token, setToken] = useState(() => localStorage.getItem(STORAGE_KEY) ?? '')
  const [draft, setDraft] = useState('')
  const [labs, setLabs] = useState<LabItem[]>([])
  const [selectedLab, setSelectedLab] = useState<string>('')
  const [scores, setScores] = useState<ScoreBucket[]>([])
  const [timeline, setTimeline] = useState<TimelineEntry[]>([])
  const [passRates, setPassRates] = useState<PassRateEntry[]>([])
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  // Fetch labs on mount when token is available
  useEffect(() => {
    if (!token) return

    const fetchLabs = async () => {
      try {
        const response = await fetch('/items/', {
          headers: { Authorization: `Bearer ${token}` },
        })
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`)
        }
        const data: LabItem[] = await response.json()
        const labItems = data.filter((item) => item.type === 'lab')
        setLabs(labItems)
        if (labItems.length > 0 && !selectedLab) {
          // Extract lab identifier from title (e.g., "Lab 04 — Testing" -> "lab-04")
          const firstLab = extractLabIdFromTitle(labItems[0].title)
          if (firstLab) {
            setSelectedLab(firstLab)
          }
        }
      } catch (err) {
        console.error('Failed to fetch labs:', err)
      }
    }

    fetchLabs()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [token])

  // Fetch analytics data when selectedLab changes
  useEffect(() => {
    if (!token || !selectedLab) return

    const fetchAnalytics = async () => {
      setLoading(true)
      setError(null)

      try {
        const [scoresRes, timelineRes, passRatesRes] = await Promise.all([
          fetch(`/analytics/scores?lab=${encodeURIComponent(selectedLab)}`, {
            headers: { Authorization: `Bearer ${token}` },
          }),
          fetch(`/analytics/timeline?lab=${encodeURIComponent(selectedLab)}`, {
            headers: { Authorization: `Bearer ${token}` },
          }),
          fetch(`/analytics/pass-rates?lab=${encodeURIComponent(selectedLab)}`, {
            headers: { Authorization: `Bearer ${token}` },
          }),
        ])

        if (!scoresRes.ok) throw new Error(`Scores: HTTP ${scoresRes.status}`)
        if (!timelineRes.ok) throw new Error(`Timeline: HTTP ${timelineRes.status}`)
        if (!passRatesRes.ok) throw new Error(`Pass rates: HTTP ${passRatesRes.status}`)

        const scoresData: ScoreBucket[] = await scoresRes.json()
        const timelineData: TimelineEntry[] = await timelineRes.json()
        const passRatesData: PassRateEntry[] = await passRatesRes.json()

        setScores(scoresData)
        setTimeline(timelineData)
        setPassRates(passRatesData)
      } catch (err) {
        const message = err instanceof Error ? err.message : 'Unknown error'
        setError(message)
      } finally {
        setLoading(false)
      }
    }

    fetchAnalytics()
  }, [token, selectedLab])

  function extractLabIdFromTitle(title: string): string | null {
    // Extract lab number from title like "Lab 04 — Testing" -> "lab-04"
    const match = title.match(/Lab\s+(\d+)/i)
    if (match) {
      return `lab-${match[1].padStart(2, '0')}`
    }
    return null
  }

  function handleConnect(e: React.FormEvent) {
    e.preventDefault()
    const trimmed = draft.trim()
    if (!trimmed) return
    localStorage.setItem(STORAGE_KEY, trimmed)
    setToken(trimmed)
  }

  function handleDisconnect() {
    localStorage.removeItem(STORAGE_KEY)
    setToken('')
    setDraft('')
    setLabs([])
    setSelectedLab('')
    setScores([])
    setTimeline([])
    setPassRates([])
  }

  function handleLabChange(e: React.ChangeEvent<HTMLSelectElement>) {
    setSelectedLab(e.target.value)
  }

  // Prepare score bucket chart data
  const scoreChartData: ChartData = {
    labels: scores.map((s) => s.bucket),
    datasets: [
      {
        label: 'Students',
        data: scores.map((s) => s.count),
        backgroundColor: 'rgba(54, 162, 235, 0.6)',
        borderColor: 'rgba(54, 162, 235, 1)',
      },
    ],
  }

  // Prepare timeline chart data
  const timelineChartData: ChartData = {
    labels: timeline.map((t) => t.date),
    datasets: [
      {
        label: 'Submissions',
        data: timeline.map((t) => t.submissions),
        borderColor: 'rgba(75, 192, 192, 1)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        fill: true,
        tension: 0.3,
      },
    ],
  }

  const chartOptions = {
    responsive: true,
    plugins: {
      legend: {
        position: 'top' as const,
      },
    },
  }

  if (!token) {
    return (
      <form className="token-form" onSubmit={handleConnect}>
        <h1>Dashboard</h1>
        <p>Enter your API key to connect.</p>
        <input
          type="password"
          placeholder="Token"
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
        />
        <button type="submit">Connect</button>
      </form>
    )
  }

  return (
    <div>
      <header className="app-header">
        <h1>Dashboard</h1>
        <button className="btn-disconnect" onClick={handleDisconnect}>
          Disconnect
        </button>
      </header>

      <div className="dashboard-controls">
        <label htmlFor="lab-select">Select Lab: </label>
        <select
          id="lab-select"
          value={selectedLab}
          onChange={handleLabChange}
          disabled={labs.length === 0}
        >
          <option value="">-- Select a lab --</option>
          {labs.map((lab) => {
            const labId = extractLabIdFromTitle(lab.title)
            return (
              <option key={lab.id} value={labId ?? lab.title}>
                {lab.title}
              </option>
            )
          })}
        </select>
      </div>

      {loading && <p>Loading analytics...</p>}
      {error && <p className="error">Error: {error}</p>}

      {!loading && !error && selectedLab && (
        <div className="dashboard-content">
          <section className="chart-section">
            <h2>Score Distribution</h2>
            <Bar data={scoreChartData} options={chartOptions} />
          </section>

          <section className="chart-section">
            <h2>Submissions Timeline</h2>
            <Line data={timelineChartData} options={chartOptions} />
          </section>

          <section className="table-section">
            <h2>Pass Rates by Task</h2>
            {passRates.length > 0 ? (
              <table>
                <thead>
                  <tr>
                    <th>Task</th>
                    <th>Avg Score</th>
                    <th>Attempts</th>
                  </tr>
                </thead>
                <tbody>
                  {passRates.map((entry) => (
                    <tr key={entry.task}>
                      <td>{entry.task}</td>
                      <td>{entry.avg_score.toFixed(1)}</td>
                      <td>{entry.attempts}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            ) : (
              <p>No pass rate data available.</p>
            )}
          </section>
        </div>
      )}

      {!loading && !error && !selectedLab && (
        <p>Please select a lab to view analytics.</p>
      )}
    </div>
  )
}

export default Dashboard
