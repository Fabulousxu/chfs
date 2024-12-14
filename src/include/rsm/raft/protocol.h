#pragma once

#include "rsm/raft/log.h"
#include "rpc/msgpack.hpp"

namespace chfs {

const std::string RAFT_RPC_START_NODE = "start node";
const std::string RAFT_RPC_STOP_NODE = "stop node";
const std::string RAFT_RPC_NEW_COMMEND = "new commend";
const std::string RAFT_RPC_CHECK_LEADER = "check leader";
const std::string RAFT_RPC_IS_STOPPED = "check stopped";
const std::string RAFT_RPC_SAVE_SNAPSHOT = "save snapshot";
const std::string RAFT_RPC_GET_SNAPSHOT = "get snapshot";

const std::string RAFT_RPC_REQUEST_VOTE = "request vote";
const std::string RAFT_RPC_APPEND_ENTRY = "append entries";
const std::string RAFT_RPC_INSTALL_SNAPSHOT = "install snapshot";

struct RequestVoteArgs {
    int term;
    int candidate_id;
    int last_log_index;
    int last_log_term;
    
    MSGPACK_DEFINE(
        term,
        candidate_id,
        last_log_index,
        last_log_term
    )
};

struct RequestVoteReply {
    int term;
    bool vote_granted;

    MSGPACK_DEFINE(
        term,
        vote_granted
    )
};

template <typename Command>
struct AppendEntriesArgs {
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    std::vector<RaftLogEntry<Command>> entries;
    int leader_commit;
};

struct RpcAppendEntriesArgs {
    int term;
    int leader_id;
    int prev_log_index;
    int prev_log_term;
    std::vector<int> entries_index;
    std::vector<int> entries_term;
    std::vector<std::vector<u8>> entries_command;
    int leader_commit;

    MSGPACK_DEFINE(
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries_index,
        entries_term,
        entries_command,
        leader_commit
    )
};

template <typename Command>
RpcAppendEntriesArgs transform_append_entries_args(const AppendEntriesArgs<Command> &arg)
{
    RpcAppendEntriesArgs rpc_arg{
        arg.term,
        arg.leader_id,
        arg.prev_log_index,
        arg.prev_log_term,
        {},
        {},
        {},
        arg.leader_commit
    };
    for (const auto &[index, term, command] : arg.entries) {
        rpc_arg.entries_index.push_back(index);
        rpc_arg.entries_term.push_back(term);
        rpc_arg.entries_command.push_back(command.serialize(command.size()));
    }
    return rpc_arg;
}

template <typename Command>
AppendEntriesArgs<Command> transform_rpc_append_entries_args(const RpcAppendEntriesArgs &rpc_arg)
{
    AppendEntriesArgs<Command> arg{
        rpc_arg.term,
        rpc_arg.leader_id,
        rpc_arg.prev_log_index,
        rpc_arg.prev_log_term,
        {},
        rpc_arg.leader_commit
    };
    for (int i = 0; i < rpc_arg.entries_index.size(); ++i) {
        Command command;
        command.deserialize(rpc_arg.entries_command[i], command.size());
        arg.entries.push_back({rpc_arg.entries_index[i], rpc_arg.entries_term[i], command});
    }
    return arg;
}

struct AppendEntriesReply {
    int term;
    bool success;

    MSGPACK_DEFINE(
        term,
        success
    )
};

struct InstallSnapshotArgs {
    int term;
    int leader_id;
    int last_included_index;
    int last_included_term;
    int offset;
    std::vector<u8> data;
    bool done;

    MSGPACK_DEFINE(
        term,
        leader_id,
        last_included_index,
        last_included_term,
        offset,
        data,
        done
    )
};

struct InstallSnapshotReply {
    int term;

    MSGPACK_DEFINE(
        term
    )
};

} /* namespace chfs */