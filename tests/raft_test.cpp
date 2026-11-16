#include "../src/raft.hpp"
#include <future>
#include <regex>
#include <gtest/gtest.h>

// Empty state machine implementation for RPC testing purposes.
class EmptyStateMachine : public StateMachine {
public:
    bool applyLogEntry(const LogEntry& entry) override {
        // For testing purposes, we can just return true to indicate that the log entry was applied
        return true;
    }
};

class RaftTransportMock : public RaftTransport {
public:
    void sendRequestVote(const std::string& peerId, const RequestVoteArgs& args, RequestVoteCallback callback) override {
        // For testing purposes, we can simulate a successful vote response with the same term and granted vote
        RequestVoteResults results;
        results.term = args.term;
        results.voteGranted = true;
        callback(results);
    }
};

TEST(GenerateUniqueServerId, CorrectlyGeneratesUUIDv7Format) {
    State state;
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    const std::string& id = raftServer.getServerId();

    const std::regex uuidV7Pattern(
        "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    );
    EXPECT_TRUE(std::regex_match(id, uuidV7Pattern));
}

TEST(GenerateUniqueServerId, GeneratesUniqueIdsForDifferentInstances) {
    State firstState;
    State secondState;
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;

    RaftServer first(firstState, stateMachine, transport, {});
    RaftServer second(secondState, stateMachine, transport, {});

    EXPECT_NE(first.getServerId(), second.getServerId());
}

TEST(RequestVote, VoteDeniedWhenCandidateLogHasLowerTermThanFollowerLog) {
    State state {
        .currentTerm = 2,
        .votedFor = std::nullopt,
        .log = {{"command1", 2}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    RequestVoteArgs args{
        .term = 1,
        .candidateId = "server1",
        .lastLogIndex = 1,
        .lastLogTerm = 1
    };

    RequestVoteResults results = raftServer.requestVote(args);

    EXPECT_EQ(results.term, 2);
    EXPECT_FALSE(results.voteGranted);
    EXPECT_EQ(state.currentTerm, 2);
    EXPECT_EQ(state.votedFor, std::nullopt);
}

TEST(RequestVote, VoteGrantedWhenCandidateLogHasHigherTermThanFollowerLog) {
    State state {
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    RequestVoteArgs args{
        .term = 2,
        .candidateId = "server1",
        .lastLogIndex = 1,
        .lastLogTerm = 1
    };

    RequestVoteResults results = raftServer.requestVote(args);

    EXPECT_EQ(results.term, 2);
    EXPECT_TRUE(results.voteGranted);
    EXPECT_EQ(state.currentTerm, 2);
    EXPECT_EQ(state.votedFor.value(), "server1");
}

TEST(RequestVote, VoteGrantedWhenCandidateLogHasSameTermButHigherIndexThanFollowerLog) {
    State state {
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    RequestVoteArgs args{
        .term = 1,
        .candidateId = "server1",
        .lastLogIndex = 2,
        .lastLogTerm = 1
    };

    RequestVoteResults results = raftServer.requestVote(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.voteGranted);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.votedFor.value(), "server1");
}

TEST(AppendEntries, EntriesNotAppendedWhenLeaderTermIsLowerThanFollowerTerm) {
    State state {
        .currentTerm = 2,
        .votedFor = std::nullopt,
        .log = {{"command1", 2}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    AppendEntriesArgs args{
        .term = 1, // Lower term than currentTerm
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command2", 1}},
        .leaderCommit = 0
    };

    AppendEntriesResults results = raftServer.appendEntries(args);

    EXPECT_EQ(results.term, 2);
    EXPECT_FALSE(results.success);
    EXPECT_EQ(state.currentTerm, 2);
    EXPECT_EQ(state.log.size(), 1); // Log should remain unchanged
}

TEST(AppendEntries, EntriesAppendedWhenPrevLogIndexAndTermMatch) {
    State state {
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    AppendEntriesArgs args{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command2", 1}},
        .leaderCommit = 0
    };

    AppendEntriesResults results = raftServer.appendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.log.size(), 2);
    EXPECT_EQ(state.log[1].command, "command2");
}

TEST(AppendEntries, EntriesNotAppendedWhenPrevLogIndexDoesNotExist) {
    State state {
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    AppendEntriesArgs args{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 2, // This index does not exist in the receiver's log
        .prevLogTerm = 1,
        .entries = {{"command3", 1}},
        .leaderCommit = 0
    };

    AppendEntriesResults results = raftServer.appendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_FALSE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.log.size(), 1); // Log should remain unchanged
}

TEST(AppendEntries, EntriesNotAppendedWhenPrevLogTermDoesNotMatch) {
    State state {
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}, {"command2", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    AppendEntriesArgs args{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 2, // This index exists in the receiver's log
        .prevLogTerm = 2, // But the term does not match
        .entries = {{"command3", 1}},
        .leaderCommit = 0
    };

    AppendEntriesResults results = raftServer.appendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_FALSE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.log.size(), 2); // Log should remain unchanged
}

TEST(AppendEntries, EntriesAppendedWhenPrevLogIndexAndTermMatchWithMultipleEntries) {
    State state {
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    AppendEntriesArgs args{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command2", 1}, {"command3", 1}},
        .leaderCommit = 0
    };

    AppendEntriesResults results = raftServer.appendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.log.size(), 3);
    EXPECT_EQ(state.log[1].command, "command2");
    EXPECT_EQ(state.log[2].command, "command3");
}

TEST(AppendEntries, EntriesAppendedWhenPrevLogIndexAndTermMatchWithConflictingEntry) {
    State state {
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}, {"command2a", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    AppendEntriesArgs args{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1, // This index exists in the receiver's log
        .prevLogTerm = 1, // And the term matches
        .entries = {{"command2b", 2}}, // Conflicting entry (different term)
        .leaderCommit = 0
    };

    AppendEntriesResults results = raftServer.appendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.log.size(), 2); // Log should be truncated to the conflicting entry
    EXPECT_EQ(state.log[1].command, "command2b");
}

TEST(AppendEntries, EntriesAppendedWhenPrevLogIndexAndTermMatchWithMultipleConflictingEntries) {
    State state {
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}, {"command2a", 1}, {"command3a", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    AppendEntriesArgs args{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1, // This index exists in the receiver's log
        .prevLogTerm = 1, // And the term matches
        .entries = {{"command2b", 2}, {"command3b", 2}}, // Conflicting entries (different term)
        .leaderCommit = 0
    };

    AppendEntriesResults results = raftServer.appendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.log.size(), 3); // Log should be truncated to the conflicting entries
    EXPECT_EQ(state.log[1].command, "command2b");
    EXPECT_EQ(state.log[2].command, "command3b");
}

TEST(AppendEntries, CommitIndexUpdatedToLeaderCommitIndexWhenLeaderCommitIndexIsSmallerThanIndexOfFollowerLastNewEntry) {
    State state {
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}, {"command2", 1}},
        .commitIndex = 0
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    AppendEntriesArgs args{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command3", 1}},
        .leaderCommit = 2
    };

    AppendEntriesResults results = raftServer.appendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.commitIndex, 2); // Follower's commit index should be updated to leader's commit index (2)
}

TEST(AppendEntries, CommitIndexUpdatedToIndexOfFollowerLastNewEntryWhenIndexOfFollowerLastNewEntryIsSmallerThanLeaderCommitIndex) {
    State state {
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}},
        .commitIndex = 0
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    AppendEntriesArgs args{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command2", 1}},
        .leaderCommit = 3
    };

    AppendEntriesResults results = raftServer.appendEntries(args);

    EXPECT_EQ(results.term, 1);
    EXPECT_TRUE(results.success);
    EXPECT_EQ(state.currentTerm, 1);
    EXPECT_EQ(state.commitIndex, 2); // Follower's commit index should be updated to index of follower's new entry (2)
}

class RecordingStateMachine : public StateMachine {
public:
    bool applyLogEntry(const LogEntry& entry) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            // Pseudo apply the log entry to the state machine by recording the command
            appliedCommands_.push_back(entry.command);
        }
        cv_.notify_all();
        return true;
    }

    bool waitForAppliedCount(std::size_t count, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this, count] {
            return appliedCommands_.size() >= count;
        });
    }

    std::vector<std::string> appliedCommands() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return appliedCommands_;
    }

private:
    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<std::string> appliedCommands_;
};

TEST(AsyncApply, AppliesCommittedEntriesInOrderWithoutDuplicates) {
    State state {.currentTerm = 1};
    RecordingStateMachine stateMachine;

    {
        RaftTransportMock transport;
        RaftServer raftServer(state, stateMachine, transport, {});

        AppendEntriesArgs firstAppend{
            .term = 1,
            .leaderId = "server1",
            .entries = {{"command1", 1}, {"command2", 1}},
            .leaderCommit = 1
        };

        AppendEntriesResults firstResults = raftServer.appendEntries(firstAppend);
        ASSERT_TRUE(firstResults.success);
        ASSERT_TRUE(stateMachine.waitForAppliedCount(1, std::chrono::seconds(1)));

        AppendEntriesArgs commitSecondEntry{
            .term = 1,
            .leaderId = "server1",
            .prevLogIndex = 2,
            .prevLogTerm = 1,
            .leaderCommit = 2
        };

        AppendEntriesResults secondResults = raftServer.appendEntries(commitSecondEntry);
        EXPECT_TRUE(secondResults.success);
        ASSERT_TRUE(stateMachine.waitForAppliedCount(2, std::chrono::seconds(1)));

        AppendEntriesResults repeatedResults = raftServer.appendEntries(commitSecondEntry);
        EXPECT_TRUE(repeatedResults.success);
    }

    EXPECT_EQ(state.lastApplied, 2);
    EXPECT_EQ(stateMachine.appliedCommands(), (std::vector<std::string>{"command1", "command2"}));
}

class BlockingStateMachine : public StateMachine {
public:
    bool applyLogEntry(const LogEntry&) override {
        std::unique_lock<std::mutex> lock(mutex_);
        started_ = true;
        cv_.notify_all();
        cv_.wait(lock, [this] {
            return released_;
        });
        return true;
    }

    bool waitUntilStarted(std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this] {
            return started_;
        });
    }

    void release() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            released_ = true;
        }
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    bool started_ = false;
    bool released_ = false;
};

TEST(AsyncApply, AppendEntriesReturnsWhileStateMachineIsBlocked) {
    State state {.currentTerm = 1};
    BlockingStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(state, stateMachine, transport, {});

    AppendEntriesArgs args{
        .term = 1,
        .leaderId = "server1",
        .entries = {{"command1", 1}},
        .leaderCommit = 1
    };

    auto result = std::async(std::launch::async, [&raftServer, args] {
        return raftServer.appendEntries(args);
    });

    const bool applyStarted = stateMachine.waitUntilStarted(std::chrono::seconds(1));
    const bool returnedWhileBlocked = result.wait_for(std::chrono::seconds(1)) == std::future_status::ready;
    stateMachine.release();

    const AppendEntriesResults appendResults = result.get();
    EXPECT_TRUE(applyStarted);
    EXPECT_TRUE(returnedWhileBlocked);
    EXPECT_TRUE(appendResults.success);
}

class DeferredRaftTransport : public RaftTransport {
public:
    struct PendingRequest {
        std::string peerId;
        RequestVoteArgs args;
        RequestVoteCallback callback;
    };

    void sendRequestVote(const std::string& peerId, const RequestVoteArgs& args, RequestVoteCallback callback) override {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            pending_.push_back({peerId, args, std::move(callback)});
        }
        cv_.notify_all();
    }

    bool waitForRequestCount(std::size_t count, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout, [this, count] {
            return pending_.size() >= count;
        });
    }

    RequestVoteArgs requestArgs(std::size_t index) {
        std::lock_guard<std::mutex> lock(mutex_);
        return pending_.at(index).args;
    }

    void respond(std::size_t index, RequestVoteResults results) {
        RequestVoteCallback callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback = pending_.at(index).callback;
        }
        callback(results);
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    std::vector<PendingRequest> pending_;
};

TEST(RequestVoteCallback, BecomesLeaderAfterMajorityGrantsVote) {
    State state;
    EmptyStateMachine stateMachine;
    DeferredRaftTransport transport;
    RaftServer raftServer(state, stateMachine, transport, {"peer-1", "peer-2"});

    ASSERT_TRUE(transport.waitForRequestCount(2, std::chrono::seconds(2)));
    const auto request = transport.requestArgs(0);
    transport.respond(0, {request.term, true});

    EXPECT_EQ(state.currentTerm, request.term);
    EXPECT_EQ(raftServer.getRaftState(), RaftState::Leader);
}

TEST(RequestVoteCallback, RejectedVoteDoesNotMakeCandidateLeader) {
    State state;
    EmptyStateMachine stateMachine;
    DeferredRaftTransport transport;
    RaftServer raftServer(state, stateMachine, transport, {"peer-1", "peer-2"});

    ASSERT_TRUE(transport.waitForRequestCount(2, std::chrono::seconds(2)));
    const auto request = transport.requestArgs(0);
    transport.respond(0, {request.term, false});

    EXPECT_EQ(state.currentTerm, request.term);
    EXPECT_NE(raftServer.getRaftState(), RaftState::Leader);
}

TEST(RequestVoteCallback, HigherTermResponseMakesServerFollower) {
    State state;
    EmptyStateMachine stateMachine;
    DeferredRaftTransport transport;
    RaftServer raftServer(state, stateMachine, transport, {"peer-1", "peer-2"});

    ASSERT_TRUE(transport.waitForRequestCount(2, std::chrono::seconds(2)));
    const auto request = transport.requestArgs(0);
    transport.respond(0, {request.term + 1, false});

    EXPECT_EQ(state.currentTerm, request.term + 1);
    EXPECT_EQ(state.votedFor, std::nullopt);
    EXPECT_EQ(raftServer.getRaftState(), RaftState::Follower);
}

TEST(RequestVoteCallback, IgnoresResponseFromOlderElection) {
    State state;
    EmptyStateMachine stateMachine;
    DeferredRaftTransport transport;
    RaftServer raftServer(state, stateMachine, transport, {"peer-1", "peer-2"});

    ASSERT_TRUE(transport.waitForRequestCount(2, std::chrono::seconds(2)));
    const auto oldRequest = transport.requestArgs(0);

    // Wait until the next election has issued its two requests.
    ASSERT_TRUE(transport.waitForRequestCount(4, std::chrono::seconds(2)));
    ASSERT_GT(state.currentTerm, oldRequest.term);
    const auto currentTerm = state.currentTerm;

    transport.respond(0, {oldRequest.term, true});

    EXPECT_EQ(state.currentTerm, currentTerm);
    EXPECT_EQ(raftServer.getRaftState(), RaftState::Candidate);
}