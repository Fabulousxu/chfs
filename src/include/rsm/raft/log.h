#pragma once

#include "common/macros.h"
#include "block/manager.h"
#include <mutex>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <sys/mman.h>

namespace chfs {

template <typename Command>
struct RaftLogEntry {
    int index;
    int term;
    Command command;
};

/** 
 * RaftLog uses a BlockManager to manage the data..
 */
template <typename Command>
class RaftLog {
public:
    RaftLog(std::shared_ptr<BlockManager> bm);
    ~RaftLog();

    void set_state(int current_term, int voted_for);
    void set_log_entries(std::vector<RaftLogEntry<Command>> &log_entries);
    void set_snapshot(std::vector<u8> &snapshot);
    void recover(int &current_term, int &voted_for, std::vector<RaftLogEntry<Command>> &log_entries, std::vector<u8> &snapshot, int &snapshot_index);

private:
    std::shared_ptr<BlockManager> bm_;
    std::mutex mtx;

    int *current_term_ptr;
    int *voted_for_ptr;
    int *log_num_ptr;
    int *snapshot_bytes_ptr;
    RaftLogEntry<Command> *log_ptr;
    u8 *snapshot_ptr;
};

template <typename Command>
RaftLog<Command>::RaftLog(std::shared_ptr<BlockManager> bm) : bm_(bm)
{
    const auto BLOCK_SIZE = bm_->block_size();
    current_term_ptr = reinterpret_cast<int *>(bm_->unsafe_get_block_ptr());
    voted_for_ptr = current_term_ptr + 1;
    log_num_ptr = current_term_ptr + 2;
    snapshot_bytes_ptr = current_term_ptr + 3;
    log_ptr = reinterpret_cast<RaftLogEntry<Command> *>(bm_->unsafe_get_block_ptr() + BLOCK_SIZE);
    snapshot_ptr = reinterpret_cast<u8 *>(bm_->unsafe_get_block_ptr() + BLOCK_SIZE * bm_->total_blocks() / 2);
}

template <typename Command>
RaftLog<Command>::~RaftLog()
{
}

template <typename Command>
void RaftLog<Command>::set_state(int current_term, int voted_for)
{
    std::unique_lock<std::mutex> lock(mtx);

    *current_term_ptr = current_term;
    *voted_for_ptr = voted_for;
    
    msync(current_term_ptr, 2 * sizeof(int), MS_SYNC | MS_INVALIDATE);
}

template <typename Command>
void RaftLog<Command>::set_log_entries(std::vector<RaftLogEntry<Command>> &log_entries)
{
    std::unique_lock<std::mutex> lock(mtx);

    *log_num_ptr = log_entries.size();
    for (int i = 0; i < log_entries.size(); i++)
        log_ptr[i] = log_entries[i];
    
    msync(log_num_ptr, sizeof(int), MS_SYNC | MS_INVALIDATE);
    msync(log_ptr, log_entries.size() * sizeof(RaftLogEntry<Command>), MS_SYNC | MS_INVALIDATE);
}

template <typename Command>
void RaftLog<Command>::set_snapshot(std::vector<u8> &snapshot)
{
    std::unique_lock<std::mutex> lock(mtx);
    
    *snapshot_bytes_ptr = snapshot.size();
    memcpy(snapshot_ptr, snapshot.data(), snapshot.size());
    
    msync(snapshot_bytes_ptr, sizeof(int), MS_SYNC | MS_INVALIDATE);   
    msync(snapshot_ptr, *snapshot_bytes_ptr, MS_SYNC | MS_INVALIDATE);
}

template <typename Command>
void RaftLog<Command>::recover(int &current_term, int &voted_for, std::vector<RaftLogEntry<Command>> &log_entries, std::vector<u8> &snapshot, int &snapshot_index) {
    std::unique_lock<std::mutex> lock(mtx);

    current_term = *current_term_ptr;
    voted_for = *voted_for_ptr;
    snapshot_index = log_ptr[0].index;

    log_entries.clear();
    for (int i = 0; i < *log_num_ptr; i++)
        log_entries.push_back(log_ptr[i]);

    snapshot.resize(*snapshot_bytes_ptr);
    memcpy(snapshot.data(), snapshot_ptr, *snapshot_bytes_ptr);
}

} /* namespace chfs */
