package raft

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

type LogVec struct {
	Logs []Log
}

func (logVec *LogVec) append(log ...Log) {
	logVec.Logs = append(logVec.Logs, log...)
}

func (logVec *LogVec) at(index int) Log {
	return logVec.Logs[index]
}

func (logVec *LogVec) lastLog() Log {
	return logVec.Logs[len(logVec.Logs)-1]
}

func (logVec *LogVec) slice(index int) []Log {
	return logVec.Logs[index:]
}

func (logVec *LogVec) truncate(index int) {
	logVec.Logs = logVec.Logs[:index]
}
