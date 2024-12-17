#pragma once

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <memory>
#include <stdarg.h>
#include <unistd.h>
#include <filesystem>

#include "rsm/state_machine.h"
#include "rsm/raft/log.h"
#include "rsm/raft/protocol.h"
#include "utils/thread_pool.h"
#include "librpc/server.h"
#include "librpc/client.h"
#include "block/manager.h"

namespace chfs {

enum class RaftRole {
    Follower,
    Candidate,
    Leader
};

struct RaftNodeConfig {
    int node_id;
    uint16_t port;
    std::string ip_address;
};

template <typename StateMachine, typename Command>
class RaftNode {

#define RAFT_LOG(fmt, args...)                                                                                   \
    do {                                                                                                         \
        auto now =                                                                                               \
            std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                std::chrono::system_clock::now().time_since_epoch())                                             \
                .count();                                                                                        \
        char buf[512];                                                                                      \
        sprintf(buf,"[%ld][%s:%d][node %d term %d role %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, role, ##args); \
        thread_pool->enqueue([=]() { std::cerr << buf;} );                                         \
    } while (0);

public:
    RaftNode (int node_id, std::vector<RaftNodeConfig> node_configs);
    ~RaftNode();

    /* interfaces for test */
    void set_network(std::map<int, bool> &network_availablility);
    void set_reliable(bool flag);
    int get_list_state_log_num();
    int rpc_count();
    std::vector<u8> get_snapshot_direct();

private:
    /* 
     * Start the raft node.
     * Please make sure all of the rpc request handlers have been registered before this method.
     */
    auto start() -> int;

    /*
     * Stop the raft node.
     */
    auto stop() -> int;
    
    /* Returns whether this node is the leader, you should also return the current term. */
    auto is_leader() -> std::tuple<bool, int>;

    /* Checks whether the node is stopped */
    auto is_stopped() -> bool;

    /* 
     * Send a new command to the raft nodes.
     * The returned tuple of the method contains three values:
     * 1. bool:  True if this raft node is the leader that successfully appends the log,
     *      false If this node is not the leader.
     * 2. int: Current term.
     * 3. int: Log index.
     */
    auto new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>;

    /* Save a snapshot of the state machine and compact the log. */
    auto save_snapshot() -> bool;

    /* Get a snapshot of the state machine */
    auto get_snapshot() -> std::vector<u8>;


    /* Internal RPC handlers */
    auto request_vote(RequestVoteArgs arg) -> RequestVoteReply;
    auto append_entries(RpcAppendEntriesArgs arg) -> AppendEntriesReply;
    auto install_snapshot(InstallSnapshotArgs arg) -> InstallSnapshotReply;

    /* RPC helpers */
    void send_request_vote(int target, RequestVoteArgs arg);
    void handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply);

    void send_append_entries(int target, AppendEntriesArgs<Command> arg);
    void handle_append_entries_reply(int target, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply);

    void send_install_snapshot(int target, InstallSnapshotArgs arg);
    void handle_install_snapshot_reply(int target, const InstallSnapshotArgs arg, const InstallSnapshotReply reply);

    /* background workers */
    void run_background_ping();
    void run_background_election();
    void run_background_commit();
    void run_background_apply();


    /* Data structures */
    bool network_stat;          /* for test */

    std::mutex mtx;                             /* A big lock to protect the whole data structure. */
    std::mutex clients_mtx;                     /* A lock to protect RpcClient pointers */
    std::unique_ptr<ThreadPool> thread_pool;
    std::unique_ptr<RaftLog<Command>> log_storage;     /* To persist the raft log. */
    std::unique_ptr<StateMachine> state;  /*  The state machine that applies the raft log, e.g. a kv store. */

    std::unique_ptr<RpcServer> rpc_server;      /* RPC server to recieve and handle the RPC requests. */
    std::map<int, std::unique_ptr<RpcClient>> rpc_clients_map;  /* RPC clients of all raft nodes including this node. */
    std::vector<RaftNodeConfig> node_configs;   /* Configuration for all nodes */ 
    int my_id;                                  /* The index of this node in rpc_clients, start from 0. */

    std::atomic_bool stopped;

    RaftRole role;
    int current_term;
    int leader_id;

    std::unique_ptr<std::thread> background_election;
    std::unique_ptr<std::thread> background_ping;
    std::unique_ptr<std::thread> background_commit;
    std::unique_ptr<std::thread> background_apply;

    int voted_for;
    int received_votes;
    int commit_index;
    int last_applied;
    int snapshot_index;
    std::chrono::system_clock::time_point election_timer;
    std::vector<int> next_index;
    std::vector<int> match_index;
    std::vector<RaftLogEntry<Command>> log_entries;
    std::vector<u8> snapshot;
    std::chrono::milliseconds ELECTION_TIMEOUT;
    const std::chrono::milliseconds BACKGROUND_ELECTION_HEARTBEAT = std::chrono::milliseconds(40);
    const std::chrono::milliseconds BACKGROUND_PING_HEARTBEAT = std::chrono::milliseconds(80);
    const std::chrono::milliseconds BACKGROUND_COMMIT_HEARTBEAT = std::chrono::milliseconds(20);
    const std::chrono::milliseconds BACKGROUND_APPLY_HEARTBEAT = std::chrono::milliseconds(10);
    
    int logic2physical(int logic_index) const { return logic_index - snapshot_index; }
    int physical2logic(int physical_index) const { return physical_index + snapshot_index; }
};

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::RaftNode(int node_id, std::vector<RaftNodeConfig> configs):
    network_stat(true),
    node_configs(configs),
    my_id(node_id),
    stopped(true),
    role(RaftRole::Follower),
    current_term(0),
    leader_id(-1),
    voted_for(-1),
    received_votes(0),
    commit_index(0),
    last_applied(0),
    snapshot_index(0),
    election_timer(std::chrono::system_clock::now()),
    next_index(configs.size()),
    match_index(configs.size())
{
    auto my_config = node_configs[my_id];

    /* launch RPC server */
    rpc_server = std::make_unique<RpcServer>(my_config.ip_address, my_config.port);

    /* Register the RPCs. */
    rpc_server->bind(RAFT_RPC_START_NODE, [this]() { return this->start(); });
    rpc_server->bind(RAFT_RPC_STOP_NODE, [this]() { return this->stop(); });
    rpc_server->bind(RAFT_RPC_CHECK_LEADER, [this]() { return this->is_leader(); });
    rpc_server->bind(RAFT_RPC_IS_STOPPED, [this]() { return this->is_stopped(); });
    rpc_server->bind(RAFT_RPC_NEW_COMMEND, [this](std::vector<u8> data, int cmd_size) { return this->new_command(data, cmd_size); });
    rpc_server->bind(RAFT_RPC_SAVE_SNAPSHOT, [this]() { return this->save_snapshot(); });
    rpc_server->bind(RAFT_RPC_GET_SNAPSHOT, [this]() { return this->get_snapshot(); });

    rpc_server->bind(RAFT_RPC_REQUEST_VOTE, [this](RequestVoteArgs arg) { return this->request_vote(arg); });
    rpc_server->bind(RAFT_RPC_APPEND_ENTRY, [this](RpcAppendEntriesArgs arg) { return this->append_entries(arg); });
    rpc_server->bind(RAFT_RPC_INSTALL_SNAPSHOT, [this](InstallSnapshotArgs arg) { return this->install_snapshot(arg); });

    const std::string LOG_FILENAME = "/tmp/raft_log/node" + std::to_string(node_id);
    bool recovered = is_file_exist(LOG_FILENAME);
    std::shared_ptr<BlockManager> bm = std::make_shared<BlockManager>(LOG_FILENAME);
    log_storage = std::make_unique<RaftLog<Command>>(bm);
    state = std::make_unique<StateMachine>();
    thread_pool = std::make_unique<ThreadPool>(4);

    if (recovered) {
        log_storage->recover(current_term, voted_for, log_entries, snapshot, snapshot_index);
        state->apply_snapshot(snapshot);
        last_applied = snapshot_index;
        commit_index = snapshot_index;
    } else {
        log_entries.push_back({0, 0, {}});
        snapshot = state->snapshot();
        log_storage->set_state(current_term, voted_for);
        log_storage->set_log_entries(log_entries);
        log_storage->set_snapshot(snapshot);
    }

    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(80, 160);
    ELECTION_TIMEOUT = std::chrono::milliseconds(dis(gen));

    rpc_server->run(true, configs.size()); 
}

template <typename StateMachine, typename Command>
RaftNode<StateMachine, Command>::~RaftNode()
{
    stop();

    thread_pool.reset();
    rpc_server.reset();
    state.reset();
    log_storage.reset();
}

/******************************************************************

                        RPC Interfaces

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::start() -> int
{
    for (const auto &[node_id, port, ip_address] : node_configs)
        rpc_clients_map[node_id] = std::make_unique<RpcClient>(ip_address, port, true);
    stopped.store(false);

    background_election = std::make_unique<std::thread>(&RaftNode::run_background_election, this);
    background_ping = std::make_unique<std::thread>(&RaftNode::run_background_ping, this);
    background_commit = std::make_unique<std::thread>(&RaftNode::run_background_commit, this);
    background_apply = std::make_unique<std::thread>(&RaftNode::run_background_apply, this);

    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::stop() -> int
{
    stopped.store(true);

    background_election->join();
    background_ping->join();
    background_commit->join();
    background_apply->join();
    return 0;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_leader() -> std::tuple<bool, int>
{
    return {role == RaftRole::Leader, current_term};
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::is_stopped() -> bool
{
    return stopped.load();
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::new_command(std::vector<u8> cmd_data, int cmd_size) -> std::tuple<bool, int, int>
{
    std::unique_lock<std::mutex> lock(mtx);

    if (role != RaftRole::Leader)
        return {false, current_term, log_entries.size()};
    
    int index = physical2logic(log_entries.size());
    Command command;
    command.deserialize(cmd_data, command.size());
    log_entries.push_back({index, current_term, command});
    log_storage->set_log_entries(log_entries);
    return {true, current_term, index};
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::save_snapshot() -> bool
{
    std::unique_lock<std::mutex> lock(mtx);

    std::vector<RaftLogEntry<Command>> new_log_entries;
    for(int i = logic2physical(last_applied); i < log_entries.size(); i++)
        new_log_entries.push_back(log_entries[i]);
    snapshot_index = last_applied;
    log_entries = new_log_entries;
    snapshot = state->snapshot();
    log_storage->set_log_entries(log_entries);
    log_storage->set_snapshot(snapshot);
    return true;
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::get_snapshot() -> std::vector<u8>
{
    return state->snapshot();
}

/******************************************************************

                         Internal RPC Related

*******************************************************************/


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::request_vote(RequestVoteArgs args) -> RequestVoteReply
{
    std::unique_lock<std::mutex> lock(mtx);
    
    if (args.term > current_term) {
        role = RaftRole::Follower;
        current_term = args.term;
        voted_for = -1;
        log_storage->set_state(current_term, voted_for);
    }

    if (args.term < current_term)
        return {current_term, false};
    
    if (voted_for != -1 && voted_for != args.candidate_id)
        return {current_term, false};

    if (log_entries[log_entries.size() - 1].term > args.last_log_term ||
        (log_entries[log_entries.size() - 1].term == args.last_log_term &&
         physical2logic(log_entries.size() - 1) > args.last_log_index))
        return {current_term, false};

    voted_for = args.candidate_id;
    log_storage->set_state(current_term, voted_for);
    election_timer = std::chrono::system_clock::now();
    return {current_term, true};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_request_vote_reply(int target, const RequestVoteArgs arg, const RequestVoteReply reply)
{
    std::unique_lock<std::mutex> lock(mtx);

    election_timer = std::chrono::system_clock::now();

    if (reply.term > current_term) {
        role = RaftRole::Follower;
        current_term = reply.term;
        voted_for = -1;
        log_storage->set_state(current_term, voted_for);
    }

    if (role != RaftRole::Candidate || !reply.vote_granted)
        return;
    
    received_votes++;
    if (received_votes <= rpc_clients_map.size() / 2)
        return;
    
    role = RaftRole::Leader;
    leader_id = my_id;
    for (int i = 0; i < node_configs.size(); i++) {
        next_index[i] = physical2logic(log_entries.size());
        match_index[i] = 0;
    }
}

template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::append_entries(RpcAppendEntriesArgs rpc_arg) -> AppendEntriesReply
{
    std::unique_lock<std::mutex> lock(mtx);

    AppendEntriesArgs<Command> arg = transform_rpc_append_entries_args<Command>(rpc_arg);

    if (arg.term > current_term) {
        role = RaftRole::Follower;
        current_term = arg.term;
        voted_for = -1;
        leader_id = arg.leader_id;
        log_storage->set_state(current_term, voted_for);
    }

    if (arg.term < current_term)
        return {current_term, false};

    election_timer = std::chrono::system_clock::now();

    if (physical2logic(log_entries.size() - 1) < arg.prev_log_index ||
        (logic2physical(arg.prev_log_index) >= 0 &&
         log_entries[logic2physical(arg.prev_log_index)].term != arg.prev_log_term))
        return {current_term, false};

    if (arg.entries.empty()) {
        if (arg.leader_commit > commit_index)
            commit_index = std::min(arg.leader_commit, arg.prev_log_index);
        return {current_term, true};
    }
    
    auto log_it = arg.entries[0].index > snapshot_index
                ? log_entries.begin() + logic2physical(arg.entries[0].index)
                : log_entries.begin() + 1;
    auto new_it = arg.entries[0].index > snapshot_index
                ? arg.entries.begin()
                : arg.entries.begin() + (snapshot_index + 1 - arg.entries[0].index);
    for (;; new_it++, log_it++) {
        if (log_it == log_entries.end()) {
            for(; new_it != arg.entries.end(); new_it++)
                log_entries.push_back(*new_it);
            break;
        }

        if (new_it == arg.entries.end())
            break;

        if (log_it->term != new_it->term) {
            log_entries.erase(log_it, log_entries.end());
            for (; new_it != arg.entries.end(); new_it++)
                log_entries.push_back(*new_it);      
            break;
        }
    }

    if (arg.leader_commit > commit_index)
        commit_index = std::min(arg.leader_commit, physical2logic(log_entries.size() - 1));
    log_storage->set_log_entries(log_entries);
    return {current_term, true};
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_append_entries_reply(int node_id, const AppendEntriesArgs<Command> arg, const AppendEntriesReply reply)
{
    std::unique_lock<std::mutex> lock(mtx);

    election_timer = std::chrono::system_clock::now();

    if (reply.term > current_term) {
        role = RaftRole::Follower;
        current_term = reply.term;
        voted_for = -1;
        log_storage->set_state(current_term, voted_for);
    }

    if (role != RaftRole::Leader)
        return;

    if (!reply.success) {
        if (reply.term <= arg.term)
            next_index[node_id]--;
        return;
    } else {
        match_index[node_id] = arg.prev_log_index + arg.entries.size();
        next_index[node_id] = match_index[node_id] + 1;

        for (int N = log_entries[log_entries.size() - 1].index; N > commit_index; N--) {
            if (log_entries[logic2physical(N)].term != current_term)
                break;

            int matched = 1;
            for (auto it = rpc_clients_map.begin(); it != rpc_clients_map.end(); it++) {
                int target_id = it->first;
                if (target_id == my_id)
                    continue;

                if (match_index[target_id] >= N)
                    matched++;

                if (matched > rpc_clients_map.size() / 2) {
                    commit_index = N;
                    break;
                }
            }
            if (commit_index == N)
                break;
        }
    }
    return;
}


template <typename StateMachine, typename Command>
auto RaftNode<StateMachine, Command>::install_snapshot(InstallSnapshotArgs args) -> InstallSnapshotReply
{
    std::unique_lock<std::mutex> lock(mtx);

    if (args.term > current_term) {
        role = RaftRole::Follower;
        current_term = args.term;
        voted_for = -1;
        leader_id = args.leader_id;
        log_storage->set_state(current_term, voted_for);
    }

    if (args.term < current_term || args.last_included_index <= snapshot_index)
        return {current_term};

    if (logic2physical(args.last_included_index) < log_entries.size() &&
        log_entries[logic2physical(args.last_included_index)].term == args.last_included_term) {
        log_entries.erase(log_entries.begin() + 1, log_entries.begin() + logic2physical(args.last_included_index) + 1);
        log_entries[0].term = args.last_included_term;
        log_entries[0].index = args.last_included_index;
        if (args.last_included_index > commit_index)
          commit_index = args.last_included_index;
    } else {
        log_entries.clear();
        log_entries.push_back({args.last_included_index, args.last_included_term, {}});
        commit_index = args.last_included_index;
    }

    snapshot_index = args.last_included_index;
    state->apply_snapshot(args.data);
    snapshot = args.data;
    last_applied = args.last_included_index;
    log_storage->set_log_entries(log_entries);
    log_storage->set_snapshot(snapshot);
    election_timer = std::chrono::system_clock::now();
    return {current_term};
}


template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::handle_install_snapshot_reply(int node_id, const InstallSnapshotArgs arg, const InstallSnapshotReply reply)
{
    std::unique_lock<std::mutex> lock(mtx);

    election_timer = std::chrono::system_clock::now();

    if (reply.term > current_term) {
        role = RaftRole::Follower;
        current_term = reply.term;
        voted_for = -1;
        log_storage->set_state(current_term, voted_for);
    }

    if (role != RaftRole::Leader || next_index[node_id] > arg.last_included_index)
        return;

    match_index[node_id] = arg.last_included_index;
    next_index[node_id] = match_index[node_id] + 1;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_request_vote(int target_id, RequestVoteArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_REQUEST_VOTE, arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_request_vote_reply(target_id, arg, res.unwrap()->as<RequestVoteReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_append_entries(int target_id, AppendEntriesArgs<Command> arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr 
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    RpcAppendEntriesArgs rpc_arg = transform_append_entries_args(arg);
    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_APPEND_ENTRY, rpc_arg);
    clients_lock.unlock();
    if (res.is_ok()) {
        handle_append_entries_reply(target_id, arg, res.unwrap()->as<AppendEntriesReply>());
    } else {
        // RPC fails
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::send_install_snapshot(int target_id, InstallSnapshotArgs arg)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    if (rpc_clients_map[target_id] == nullptr
        || rpc_clients_map[target_id]->get_connection_state() != rpc::client::connection_state::connected) {
        return;
    }

    auto res = rpc_clients_map[target_id]->call(RAFT_RPC_INSTALL_SNAPSHOT, arg);
    clients_lock.unlock();
    if (res.is_ok()) { 
        handle_install_snapshot_reply(target_id, arg, res.unwrap()->as<InstallSnapshotReply>());
    } else {
        // RPC fails
    }
}


/******************************************************************

                        Background Workers

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.

    while (true) {
        {
            if (is_stopped()) {
                return;
            }

            std::unique_lock<std::mutex> lock(mtx);

            if ((role == RaftRole::Follower || role == RaftRole::Candidate) &&
                std::chrono::system_clock::now() - election_timer > ELECTION_TIMEOUT) {
                role = RaftRole::Candidate;
                current_term++;
                voted_for = my_id;
                received_votes = 1;
                election_timer = std::chrono::system_clock::now();

                for (auto it = rpc_clients_map.begin(); it != rpc_clients_map.end(); it++) {
                    int target_id = it->first;
                    if (target_id == my_id)
                        continue;

                    RequestVoteArgs arg{
                        current_term,
                        my_id,
                        log_entries[log_entries.size() - 1].index,
                        log_entries[log_entries.size() - 1].term
                    };

                    thread_pool->enqueue([=] { send_request_vote(target_id, arg); });
                }

                log_storage->set_state(current_term, voted_for);
            }

            lock.unlock();
            std::this_thread::sleep_for(BACKGROUND_ELECTION_HEARTBEAT);
        }
    }
    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            
            std::unique_lock<std::mutex> lock(mtx);

            if (role == RaftRole::Leader) {
                for (auto it = rpc_clients_map.begin(); it != rpc_clients_map.end(); it++) {
                    int target_id = it->first;
                    if (target_id == my_id)
                        continue;

                    if (next_index[target_id] < physical2logic(log_entries.size())) {
                        if (logic2physical(next_index[target_id]) > 0) {
                            std::vector<RaftLogEntry<Command>> entries;
                            for (int i = logic2physical(next_index[it->first]); i < log_entries.size(); i++)
                                entries.push_back(log_entries[i]);

                            AppendEntriesArgs<Command> args{
                                current_term,
                                my_id,
                                next_index[target_id] - 1,
                                log_entries[logic2physical(args.prev_log_index)].term,
                                entries,
                                commit_index
                            };

                            thread_pool->enqueue([=] { send_append_entries(target_id, args); });
                        } else {
                            InstallSnapshotArgs args{
                                current_term,
                                my_id,
                                snapshot_index,
                                log_entries[logic2physical(snapshot_index)].term,
                                0,
                                snapshot,
                                true
                            };

                            thread_pool->enqueue([=] { send_install_snapshot(target_id, args); });
                        }
                    }
                }
            }

            lock.unlock();
            std::this_thread::sleep_for(BACKGROUND_COMMIT_HEARTBEAT);
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    while (true) {
        {
            if (is_stopped()) {
                return;
            }
            
            std::unique_lock<std::mutex> lock(mtx);

            if (last_applied < commit_index) {
                for (int i = logic2physical(last_applied + 1); i <= logic2physical(commit_index); i++)
                    state->apply_log(log_entries[i].command);
                last_applied = commit_index;
            }

            lock.unlock();
            std::this_thread::sleep_for(BACKGROUND_APPLY_HEARTBEAT);
        }
    }

    return;
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.

    while (true) {
        {
            if (is_stopped()) {
                return;
            }

            std::unique_lock<std::mutex> lock(mtx);

            if (role == RaftRole::Leader) {
                for (auto it = rpc_clients_map.begin(); it != rpc_clients_map.end(); it++) {
                    int target_id = it->first;
                    if (target_id == my_id)
                        continue;

                    if (logic2physical(next_index[target_id]) > 0) {
                        AppendEntriesArgs<Command> args{
                            current_term,
                            my_id,
                            next_index[target_id] - 1,
                            log_entries[logic2physical(args.prev_log_index)].term,
                            {},
                            commit_index
                        };

                        thread_pool->enqueue([=] { send_append_entries(target_id, args); });
                    } else {
                        InstallSnapshotArgs args{
                            current_term,
                            my_id,
                            snapshot_index,
                            log_entries[logic2physical(snapshot_index)].term,
                            0,
                            snapshot,
                            true
                        };

                        thread_pool->enqueue([=] { send_install_snapshot(target_id, args); });
                    }
                }
            }

            lock.unlock();
            std::this_thread::sleep_for(BACKGROUND_PING_HEARTBEAT);
        }
    }
    return;
}

/******************************************************************

                          Test Functions (must not edit)

*******************************************************************/

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_network(std::map<int, bool> &network_availability)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    /* turn off network */
    if (!network_availability[my_id]) {
        for (auto &&client: rpc_clients_map) {
            if (client.second != nullptr)
                client.second.reset();
        }

        return;
    }

    for (auto node_network: network_availability) {
        int node_id = node_network.first;
        bool node_status = node_network.second;

        if (node_status && rpc_clients_map[node_id] == nullptr) {
            RaftNodeConfig target_config;
            for (auto config: node_configs) {
                if (config.node_id == node_id) 
                    target_config = config;
            }

            rpc_clients_map[node_id] = std::make_unique<RpcClient>(target_config.ip_address, target_config.port, true);
        }

        if (!node_status && rpc_clients_map[node_id] != nullptr) {
            rpc_clients_map[node_id].reset();
        }
    }
}

template <typename StateMachine, typename Command>
void RaftNode<StateMachine, Command>::set_reliable(bool flag)
{
    std::unique_lock<std::mutex> clients_lock(clients_mtx);
    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            client.second->set_reliable(flag);
        }
    }
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::get_list_state_log_num()
{
    /* only applied to ListStateMachine*/
    std::unique_lock<std::mutex> lock(mtx);

    return state->num_append_logs;
}

template <typename StateMachine, typename Command>
int RaftNode<StateMachine, Command>::rpc_count()
{
    int sum = 0;
    std::unique_lock<std::mutex> clients_lock(clients_mtx);

    for (auto &&client: rpc_clients_map) {
        if (client.second) {
            sum += client.second->count();
        }
    }
    
    return sum;
}

template <typename StateMachine, typename Command>
std::vector<u8> RaftNode<StateMachine, Command>::get_snapshot_direct()
{
    if (is_stopped()) {
        return std::vector<u8>();
    }

    std::unique_lock<std::mutex> lock(mtx);

    return state->snapshot(); 
}

}