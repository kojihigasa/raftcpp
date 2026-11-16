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

TEST(GenerateUniqueServerId, ReturnsUuidV7FormattedId) {
    State initialState{};
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const std::string serverId = raftServer.getServerId();

    const std::regex uuidV7Format(
        "^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
    );
    EXPECT_TRUE(std::regex_match(serverId, uuidV7Format));
}

TEST(GenerateUniqueServerId, GeneratesDistinctIdsForSeparateServers) {
    State firstInitialState{};
    State secondInitialState{};
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;

    RaftServer firstServer(firstInitialState, stateMachine, transport, {});
    RaftServer secondServer(secondInitialState, stateMachine, transport, {});

    const std::string firstServerId = firstServer.getServerId();
    const std::string secondServerId = secondServer.getServerId();
    EXPECT_NE(firstServerId, secondServerId);
}

TEST(RequestVote, RejectsCandidateWithOutdatedLog) {
    State initialState{
        .currentTerm = 2,
        .votedFor = std::nullopt,
        .log = {{"command1", 2}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const RequestVoteArgs request{
        .term = 2,
        .candidateId = "server1",
        .lastLogIndex = 1,
        .lastLogTerm = 1
    };

    const RequestVoteResults response = raftServer.requestVote(request);
    const State stateAfterRequest = raftServer.getState();

    EXPECT_EQ(response.term, 2);
    EXPECT_FALSE(response.voteGranted);
    EXPECT_EQ(stateAfterRequest.currentTerm, 2);
    EXPECT_FALSE(stateAfterRequest.votedFor.has_value());
}

TEST(RequestVote, GrantsVoteAndAdvancesTermForNewerCandidate) {
    State initialState{
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const RequestVoteArgs request{
        .term = 2,
        .candidateId = "server1",
        .lastLogIndex = 1,
        .lastLogTerm = 1
    };

    const RequestVoteResults response = raftServer.requestVote(request);
    const State stateAfterRequest = raftServer.getState();

    EXPECT_EQ(response.term, 2);
    EXPECT_TRUE(response.voteGranted);
    EXPECT_EQ(stateAfterRequest.currentTerm, 2);
    ASSERT_TRUE(stateAfterRequest.votedFor.has_value());
    EXPECT_EQ(stateAfterRequest.votedFor.value(), "server1");
}

TEST(RequestVote, GrantsVoteToCandidateWithLongerLogAtSameTerm) {
    State initialState{
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const RequestVoteArgs request{
        .term = 1,
        .candidateId = "server1",
        .lastLogIndex = 2,
        .lastLogTerm = 1
    };

    const RequestVoteResults response = raftServer.requestVote(request);
    const State stateAfterRequest = raftServer.getState();

    EXPECT_EQ(response.term, 1);
    EXPECT_TRUE(response.voteGranted);
    EXPECT_EQ(stateAfterRequest.currentTerm, 1);
    ASSERT_TRUE(stateAfterRequest.votedFor.has_value());
    EXPECT_EQ(stateAfterRequest.votedFor.value(), "server1");
}

TEST(AppendEntries, RejectsAppendFromOlderTerm) {
    State initialState{
        .currentTerm = 2,
        .votedFor = std::nullopt,
        .log = {{"command1", 2}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const AppendEntriesArgs appendArgs{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command2", 1}},
        .leaderCommit = 0
    };

    const AppendEntriesResults response = raftServer.appendEntries(appendArgs);
    const State stateAfterAppend = raftServer.getState();

    EXPECT_EQ(response.term, 2);
    EXPECT_FALSE(response.success);
    EXPECT_EQ(stateAfterAppend.currentTerm, 2);
    EXPECT_EQ(stateAfterAppend.log.size(), 1);
}

TEST(AppendEntries, AppendsEntriesWhenPreviousLogMatches) {
    State initialState{
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const AppendEntriesArgs appendArgs{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command2", 1}},
        .leaderCommit = 0
    };

    const AppendEntriesResults response = raftServer.appendEntries(appendArgs);
    const State stateAfterAppend = raftServer.getState();

    EXPECT_EQ(response.term, 1);
    EXPECT_TRUE(response.success);
    EXPECT_EQ(stateAfterAppend.currentTerm, 1);
    EXPECT_EQ(stateAfterAppend.log.size(), 2);
    EXPECT_EQ(stateAfterAppend.log[1].command, "command2");
}

TEST(AppendEntries, RejectsAppendWhenPreviousIndexIsMissing) {
    State initialState{
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const AppendEntriesArgs appendArgs{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 2,
        .prevLogTerm = 1,
        .entries = {{"command3", 1}},
        .leaderCommit = 0
    };

    const AppendEntriesResults response = raftServer.appendEntries(appendArgs);
    const State stateAfterAppend = raftServer.getState();

    EXPECT_EQ(response.term, 1);
    EXPECT_FALSE(response.success);
    EXPECT_EQ(stateAfterAppend.currentTerm, 1);
    EXPECT_EQ(stateAfterAppend.log.size(), 1);
}

TEST(AppendEntries, RejectsAppendWhenPreviousTermDiffers) {
    State initialState{
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}, {"command2", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const AppendEntriesArgs appendArgs{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 2,
        .prevLogTerm = 2,
        .entries = {{"command3", 1}},
        .leaderCommit = 0
    };

    const AppendEntriesResults response = raftServer.appendEntries(appendArgs);
    const State stateAfterAppend = raftServer.getState();
    EXPECT_EQ(response.term, 1);
    EXPECT_FALSE(response.success);
    EXPECT_EQ(stateAfterAppend.currentTerm, 1);
    EXPECT_EQ(stateAfterAppend.log.size(), 2);
}

TEST(AppendEntries, AppendsMultipleEntriesWhenPreviousLogMatches) {
    State initialState{
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const AppendEntriesArgs appendArgs{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command2", 1}, {"command3", 1}},
        .leaderCommit = 0
    };

    const AppendEntriesResults response = raftServer.appendEntries(appendArgs);
    const State stateAfterAppend = raftServer.getState();

    EXPECT_EQ(response.term, 1);
    EXPECT_TRUE(response.success);
    EXPECT_EQ(stateAfterAppend.currentTerm, 1);
    EXPECT_EQ(stateAfterAppend.log.size(), 3);
    EXPECT_EQ(stateAfterAppend.log[1].command, "command2");
    EXPECT_EQ(stateAfterAppend.log[2].command, "command3");
}

TEST(AppendEntries, ReplacesConflictingEntry) {
    State initialState{
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}, {"command2a", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const AppendEntriesArgs appendArgs{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command2b", 2}},
        .leaderCommit = 0
    };

    const AppendEntriesResults response = raftServer.appendEntries(appendArgs);
    const State stateAfterAppend = raftServer.getState();

    EXPECT_EQ(response.term, 1);
    EXPECT_TRUE(response.success);
    EXPECT_EQ(stateAfterAppend.currentTerm, 1);
    EXPECT_EQ(stateAfterAppend.log.size(), 2);
    EXPECT_EQ(stateAfterAppend.log[1].command, "command2b");
}

TEST(AppendEntries, ReplacesMultipleConflictingEntries) {
    State initialState{
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}, {"command2a", 1}, {"command3a", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const AppendEntriesArgs appendArgs{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command2b", 2}, {"command3b", 2}},
        .leaderCommit = 0
    };

    const AppendEntriesResults response = raftServer.appendEntries(appendArgs);
    const State stateAfterAppend = raftServer.getState();
    EXPECT_EQ(response.term, 1);
    EXPECT_TRUE(response.success);
    EXPECT_EQ(stateAfterAppend.currentTerm, 1);
    EXPECT_EQ(stateAfterAppend.log.size(), 3);
    EXPECT_EQ(stateAfterAppend.log[1].command, "command2b");
    EXPECT_EQ(stateAfterAppend.log[2].command, "command3b");
}

TEST(AppendEntries, DoesNotDuplicateEntriesWhenRequestIsRepeated) {
    State initialState{
        .currentTerm = 1,
        .log = {{"command1", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const AppendEntriesArgs appendArgs{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command2", 1}},
        .leaderCommit = 0
    };

    const AppendEntriesResults firstResponse = raftServer.appendEntries(appendArgs);
    const AppendEntriesResults retryResponse = raftServer.appendEntries(appendArgs);
    const State stateAfterRetry = raftServer.getState();

    EXPECT_TRUE(firstResponse.success);
    EXPECT_TRUE(retryResponse.success);
    ASSERT_EQ(stateAfterRetry.log.size(), 2);
    EXPECT_EQ(stateAfterRetry.log[1].command, "command2");
}

TEST(AppendEntries, AppendsOnlyEntriesMissingFromOverlappingRequest) {
    State initialState{
        .currentTerm = 1,
        .log = {{"command1", 1}, {"command2", 1}}
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const AppendEntriesArgs appendArgs{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command2", 1}, {"command3", 1}},
        .leaderCommit = 0
    };

    const AppendEntriesResults response = raftServer.appendEntries(appendArgs);
    const State stateAfterAppend = raftServer.getState();

    EXPECT_TRUE(response.success);
    ASSERT_EQ(stateAfterAppend.log.size(), 3);
    EXPECT_EQ(stateAfterAppend.log[1].command, "command2");
    EXPECT_EQ(stateAfterAppend.log[2].command, "command3");
}

TEST(AppendEntries, CommitIndexFollowsLeaderCommitWhenWithinLog) {
    State initialState{
        .currentTerm = 1,
        .votedFor = std::nullopt,
        .log = {{"command1", 1}, {"command2", 1}},
        .commitIndex = 0
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const AppendEntriesArgs appendArgs{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command3", 1}},
        .leaderCommit = 2
    };

    const AppendEntriesResults response = raftServer.appendEntries(appendArgs);
    const State stateAfterAppend = raftServer.getState();

    EXPECT_EQ(response.term, 1);
    EXPECT_TRUE(response.success);
    EXPECT_EQ(stateAfterAppend.currentTerm, 1);
    EXPECT_EQ(stateAfterAppend.commitIndex, 2);
}

TEST(AppendEntries, CommitIndexIsCappedAtLastLogIndex) {
    State initialState{
        .currentTerm = 1,
        .log = {{"command1", 1}},
        .commitIndex = 0
    };
    EmptyStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const AppendEntriesArgs appendArgs{
        .term = 1,
        .leaderId = "server1",
        .prevLogIndex = 1,
        .prevLogTerm = 1,
        .entries = {{"command2", 1}},
        .leaderCommit = 3
    };

    const AppendEntriesResults response = raftServer.appendEntries(appendArgs);
    const State stateAfterAppend = raftServer.getState();

    EXPECT_TRUE(response.success);
    EXPECT_EQ(response.term, 1);
    EXPECT_EQ(stateAfterAppend.currentTerm, 1);
    EXPECT_EQ(stateAfterAppend.log.size(), 2);
    EXPECT_EQ(stateAfterAppend.commitIndex, 2);
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
    State initialState{.currentTerm = 1};
    RecordingStateMachine stateMachine;
    State stateAfterApply{};

    {
        RaftTransportMock transport;
        RaftServer raftServer(initialState, stateMachine, transport, {});

        const AppendEntriesArgs appendTwoEntries{
            .term = 1,
            .leaderId = "server1",
            .entries = {{"command1", 1}, {"command2", 1}},
            .leaderCommit = 1
        };
        const AppendEntriesResults firstResponse = raftServer.appendEntries(appendTwoEntries);
        ASSERT_TRUE(firstResponse.success);
        ASSERT_TRUE(stateMachine.waitForAppliedCount(1, std::chrono::seconds(1)));

        const AppendEntriesArgs commitSecondEntry{
            .term = 1,
            .leaderId = "server1",
            .prevLogIndex = 2,
            .prevLogTerm = 1,
            .leaderCommit = 2
        };
        const AppendEntriesResults secondResponse = raftServer.appendEntries(commitSecondEntry);
        ASSERT_TRUE(secondResponse.success);
        ASSERT_TRUE(stateMachine.waitForAppliedCount(2, std::chrono::seconds(1)));

        // Repeating the same commit must not apply either entry again.
        const AppendEntriesResults repeatedResponse = raftServer.appendEntries(commitSecondEntry);
        ASSERT_TRUE(repeatedResponse.success);
        stateAfterApply = raftServer.getState();
    }

    // Destroying RaftServer joins its apply worker before inspecting final effects.
    EXPECT_EQ(stateAfterApply.lastApplied, 2);
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
    State initialState{.currentTerm = 1};
    BlockingStateMachine stateMachine;
    RaftTransportMock transport;
    RaftServer raftServer(initialState, stateMachine, transport, {});

    const AppendEntriesArgs appendArgs{
        .term = 1,
        .leaderId = "server1",
        .entries = {{"command1", 1}},
        .leaderCommit = 1
    };

    auto appendFuture = std::async(std::launch::async, [&raftServer, appendArgs] {
        return raftServer.appendEntries(appendArgs);
    });

    const auto timeout = std::chrono::seconds(1);
    const bool stateMachineStarted = stateMachine.waitUntilStarted(timeout);
    const bool appendReturnedBeforeApplyFinished =
        appendFuture.wait_for(timeout) == std::future_status::ready;

    // Always unblock the worker before waiting on the future or making assertions.
    stateMachine.release();
    const AppendEntriesResults appendResults = appendFuture.get();

    EXPECT_TRUE(stateMachineStarted);
    EXPECT_TRUE(appendReturnedBeforeApplyFinished);
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
    State initialState{};
    EmptyStateMachine stateMachine;
    DeferredRaftTransport transport;
    const std::vector<std::string> peerIds{"peer-1", "peer-2"};
    RaftServer raftServer(initialState, stateMachine, transport, peerIds);

    ASSERT_TRUE(transport.waitForRequestCount(peerIds.size(), std::chrono::seconds(2)));
    ASSERT_EQ(raftServer.getRaftState(), RaftState::Candidate);

    const RequestVoteArgs request = transport.requestArgs(0);
    transport.respond(0, {request.term, true});

    const State stateAfterResponse = raftServer.getState();
    EXPECT_EQ(stateAfterResponse.currentTerm, request.term);
    EXPECT_EQ(raftServer.getRaftState(), RaftState::Leader);
}

TEST(RequestVoteCallback, RejectedVoteDoesNotMakeCandidateLeader) {
    State initialState{};
    EmptyStateMachine stateMachine;
    DeferredRaftTransport transport;
    const std::vector<std::string> peerIds{"peer-1", "peer-2"};
    RaftServer raftServer(initialState, stateMachine, transport, peerIds);

    ASSERT_TRUE(transport.waitForRequestCount(peerIds.size(), std::chrono::seconds(2)));
    ASSERT_EQ(raftServer.getRaftState(), RaftState::Candidate);

    const RequestVoteArgs request = transport.requestArgs(0);
    transport.respond(0, {request.term, false});

    const State stateAfterResponse = raftServer.getState();
    EXPECT_EQ(stateAfterResponse.currentTerm, request.term);
    EXPECT_EQ(raftServer.getRaftState(), RaftState::Candidate);
}

TEST(RequestVoteCallback, HigherTermResponseMakesServerFollower) {
    State initialState{};
    EmptyStateMachine stateMachine;
    DeferredRaftTransport transport;
    const std::vector<std::string> peerIds{"peer-1", "peer-2"};
    RaftServer raftServer(initialState, stateMachine, transport, peerIds);

    ASSERT_TRUE(transport.waitForRequestCount(peerIds.size(), std::chrono::seconds(2)));
    ASSERT_EQ(raftServer.getRaftState(), RaftState::Candidate);

    const std::uint64_t electionTerm = transport.requestArgs(0).term;
    const std::uint64_t higherTerm = electionTerm + 1;
    transport.respond(0, {higherTerm, false});

    const State stateAfterResponse = raftServer.getState();
    EXPECT_EQ(stateAfterResponse.currentTerm, higherTerm);
    EXPECT_FALSE(stateAfterResponse.votedFor.has_value());
    EXPECT_EQ(raftServer.getRaftState(), RaftState::Follower);
}

TEST(RequestVoteCallback, IgnoresResponseFromOlderElection) {
    State initialState{};
    EmptyStateMachine stateMachine;
    DeferredRaftTransport transport;
    const std::vector<std::string> peerIds{"peer-1", "peer-2"};
    const std::size_t requestsPerElection = peerIds.size();
    RaftServer raftServer(initialState, stateMachine, transport, peerIds);

    ASSERT_TRUE(transport.waitForRequestCount(requestsPerElection, std::chrono::seconds(2)));
    const std::uint64_t oldElectionTerm = transport.requestArgs(0).term;

    // A second request round indicates that a newer election has started.
    ASSERT_TRUE(transport.waitForRequestCount(2 * requestsPerElection, std::chrono::seconds(2)));
    const State stateBeforeOldResponse = raftServer.getState();
    ASSERT_GT(stateBeforeOldResponse.currentTerm, oldElectionTerm);
    ASSERT_EQ(raftServer.getRaftState(), RaftState::Candidate);

    transport.respond(0, {oldElectionTerm, true});

    const State stateAfterOldResponse = raftServer.getState();
    EXPECT_EQ(stateAfterOldResponse.currentTerm, stateBeforeOldResponse.currentTerm);
    EXPECT_EQ(raftServer.getRaftState(), RaftState::Candidate);
}

TEST(RequestVoteCallback, IgnoresDuplicateResponsesFromSamePeer) {
    State initialState{};
    EmptyStateMachine stateMachine;
    DeferredRaftTransport transport;
    const std::vector<std::string> peerIds{"peer-1", "peer-2", "peer-3", "peer-4"};
    RaftServer raftServer(initialState, stateMachine, transport, peerIds);

    ASSERT_TRUE(transport.waitForRequestCount(peerIds.size(), std::chrono::seconds(2)));
    ASSERT_EQ(raftServer.getRaftState(), RaftState::Candidate);

    const RequestVoteArgs request = transport.requestArgs(0);
    transport.respond(0, {request.term, true});
    transport.respond(0, {request.term, true}); // Duplicate response from the same peer
    EXPECT_EQ(raftServer.getRaftState(), RaftState::Candidate);

    transport.respond(1, {request.term, true});
    EXPECT_EQ(raftServer.getRaftState(), RaftState::Leader);
}
