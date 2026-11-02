#pragma once

#include <condition_variable>
#include <cstddef>
#include <queue>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

struct LogEntry {
    std::string command;
    int term;
};

struct State {
    int currentTerm = 0;
    std::optional<int> votedFor;
    std::vector<LogEntry> log;
    // A log entry is committed once the leader that created the entry has replicated it on a majority of the servers
    int commitIndex = 0;
    int lastApplied = 0;
    std::vector<int> nextIndex;
    std::vector<int> matchIndex;

    void initializeLeaderState(std::size_t serverCount) {
        const int nextLogIndex = static_cast<int>(log.size()) + 1;
        nextIndex.assign(serverCount, nextLogIndex);
        matchIndex.assign(serverCount, 0);
    }
};

enum class RaftState : int {
    Follower,
    Candidate,
    Leader
};

class StateMachine {
public:
    StateMachine() = default;
    virtual ~StateMachine() = default;

    // This method should be implemented by the user to apply a log entry to the state machine.
    virtual bool applyLogEntry(const LogEntry& entry) = 0;
};

struct RequestVoteArgs {
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;
};

struct RequestVoteResults {
    int term;
    bool voteGranted;
};

struct AppendEntriesArgs {
    int term;
    int leaderId;
    int prevLogIndex;
    int prevLogTerm;
    std::vector<LogEntry> entries;
    int leaderCommit;
};

struct AppendEntriesResults {
    int term;
    bool success;
};

class RaftServer {
public:
    RaftServer(State& state, StateMachine& stateMachine);
    ~RaftServer();

    RequestVoteResults handleRequestVote(const RequestVoteArgs& args);
    AppendEntriesResults handleAppendEntries(const AppendEntriesArgs& args);

private:
    void applyLoop();
    void enqueueCommittedEntries();

    State& state_;
    StateMachine& stateMachine_;

    RaftState raftState_ = RaftState::Follower;

    // Private members for asynchronous application of committed log entries to the state machine
    std::mutex mutex_;
    std::condition_variable cv_;
    std::queue<std::pair<int, LogEntry>> applyQueue_;
    int lastEnqueued_ = 0; // The index of the last log entry that has been enqueued for application to the state machine
    bool terminating_ = false; // Flag to indicate that the RaftServer is being destroyed and the applyLoop should terminate
    std::optional<int> applyFailed_; // The index of the first log entry that failed to be applied to the state machine
    std::thread applyThread_; // Declare at the end to ensure proper construction order
};