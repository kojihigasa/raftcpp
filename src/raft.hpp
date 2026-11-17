#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <queue>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using ServerId = std::string;

struct LogEntry {
    std::string command;
    std::uint64_t term;
};

struct State {
    std::uint64_t currentTerm = 0;
    std::optional<ServerId> votedFor;
    std::vector<LogEntry> log;
    // A log entry is committed once the leader that created the entry has replicated it on a majority of the servers
    std::uint64_t commitIndex = 0;
    std::uint64_t lastApplied = 0;
    std::vector<std::uint64_t> nextIndex;
    std::vector<std::uint64_t> matchIndex;

    void initializeLeaderState(std::size_t serverCount) {
        const std::uint64_t nextLogIndex = static_cast<std::uint64_t>(log.size()) + 1;
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
    virtual ~StateMachine() = default;

    // This method should be implemented by the user to apply a log entry to the state machine.
    virtual bool applyLogEntry(const LogEntry& entry) = 0;
};

struct RequestVoteArgs {
    std::uint64_t term;
    ServerId candidateId;
    std::uint64_t lastLogIndex;
    std::uint64_t lastLogTerm;
};

struct RequestVoteResults {
    std::uint64_t term;
    bool voteGranted;
};

struct AppendEntriesArgs {
    std::uint64_t term;
    ServerId leaderId;
    std::uint64_t prevLogIndex;
    std::uint64_t prevLogTerm;
    std::vector<LogEntry> entries;
    std::uint64_t leaderCommit;
};

struct AppendEntriesResults {
    std::uint64_t term;
    bool success;
};

using RequestVoteCallback = std::function<void(RequestVoteResults)>;

struct RaftConfig {
    std::chrono::milliseconds minElectionTimeout{150};
    std::chrono::milliseconds maxElectionTimeout{300};
};

class RaftTransport {
public:
    virtual ~RaftTransport() = default;

    // This method should be implemented by the user to send a RequestVote RPC to another server.
    virtual void sendRequestVote(const ServerId& peerId, const RequestVoteArgs& args, RequestVoteCallback callback) = 0;
};

class RaftServer {
public:
    RaftServer(
        State initialState,
        StateMachine& stateMachine,
        RaftTransport& transport,
        std::vector<ServerId> peerIds,
        RaftConfig config = {}
    );
    ~RaftServer();

    const ServerId& getServerId() const noexcept {
        return serverId_;
    }

    RequestVoteResults requestVote(const RequestVoteArgs& args);
    AppendEntriesResults appendEntries(const AppendEntriesArgs& args);

    State getState() const {
        std::lock_guard<std::mutex> lock(stateMutex_);
        return state_;
    }

    RaftState getRaftState() const {
        std::lock_guard<std::mutex> lock(stateMutex_);
        return raftState_;
    }

private:
    void applyLoop();
    void enqueueCommittedEntries();

    void electionTimeoutLoop();
    std::chrono::milliseconds getRandomElectionTimeout() const;
    void resetElectionTimer();
    void stopElectionTimer();
    void startElection(std::uint64_t expectedTimerGeneration);

    static ServerId generateUniqueServerId();

    State state_;
    StateMachine& stateMachine_;
    RaftTransport& transport_;
    std::vector<ServerId> peerIds_;
    RaftConfig config_;

    RaftState raftState_ = RaftState::Follower;

    // Mutex to protect access to the Raft state
    // Note: mutability is used to allow const getter to lock the mutex for thread-safe access to the Raft state.
    mutable std::mutex stateMutex_;

    // Unique identifier for the server
    ServerId serverId_;
    // Flag to indicate that the RaftServer is being destroyed and the applyLoop should terminate
    std::atomic<bool> terminating_ = false;

    // Private members for asynchronous application of committed log entries to the state machine
    std::mutex applyMutex_;
    std::condition_variable applyCv_;
    std::queue<std::pair<std::uint64_t, LogEntry>> applyQueue_;
    std::uint64_t lastEnqueued_ = 0; // The index of the last log entry that has been enqueued for application to the state machine
    std::optional<std::uint64_t> applyFailed_; // The index of the first log entry that failed to be applied to the state machine
    std::thread applyThread_; // Declare at the end to ensure proper construction order

    // Private memebers for election timeout detection
    std::mutex electionMutex_;
    std::condition_variable electionCv_;
    std::chrono::milliseconds electionTimeout_; // Base election timeout duration
    bool electionTimerEnabled_ = true;
    std::uint64_t timerGeneration_ = 0; // A counter to track the number of times the election timer has been reset
    std::thread electionTimeoutThread_; // Declare at the end to ensure proper construction order
};
