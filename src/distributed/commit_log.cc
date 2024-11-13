#include <algorithm>

#include "common/bitmap.h"
#include "distributed/commit_log.h"
#include "distributed/metadata_server.h"
#include "filesystem/directory_op.h"
#include "metadata/inode.h"
#include <chrono>

namespace chfs {
/**
 * `CommitLog` part
 */
// {Your code here}
CommitLog::CommitLog(std::shared_ptr<BlockManager> bm,
                     bool is_checkpoint_enabled)
    : is_checkpoint_enabled_(is_checkpoint_enabled), bm_(bm) {
  const auto block_size = bm->block_size();
  auto begin_ptr = bm->unsafe_get_block_ptr() +
                   (bm->total_blocks() - kLogBlockCnt) * block_size;
  this->log_data = reinterpret_cast<LogEntry *>(begin_ptr);
  this->max_log_entry_num = kLogBlockCnt * block_size / sizeof(LogEntry);
  this->log_entry_num = 0;
  this->begin_txn_id = 0;
  this->last_txn_id = 0;
}

CommitLog::~CommitLog() {}

// {Your code here}
auto CommitLog::get_log_entry_num() -> usize { 
  return this->last_txn_id - this->begin_txn_id;
}

auto CommitLog::gen_txn_id() -> txn_id_t { return this->last_txn_id + 1; }

// {Your code here}
auto CommitLog::append_log(txn_id_t txn_id,
                           std::vector<std::shared_ptr<BlockOperation>> ops)
    -> void {
  std::unique_lock<std::mutex> lock(this->mutex_);

  CHFS_ASSERT(this->log_entry_num + ops.size() <= this->max_log_entry_num,
              "Log full!");
  
  for (auto &op : ops) {
    auto &entry = this->log_data[this->log_entry_num];
    entry.txn_id = txn_id;
    entry.block_id = op->block_id_;
    memcpy(entry.block_state, op->new_block_state_.data(), bm_->block_size());
    this->log_entry_num++;
  }

  auto flush_res = bm_->flush_log();
  CHFS_ASSERT(flush_res.is_ok(), "Failed to flush log");

  this->last_txn_id = txn_id;
}

// {Your code here}
auto CommitLog::commit_log(txn_id_t txn_id) -> void {
  std::unique_lock<std::mutex> lock(this->mutex_);

  for (int i = 0; i < log_entry_num; i++) {
    if (log_data[i].txn_id == txn_id) {
      log_data[i].block_id = KInvalidBlockID;
    }
  }

  auto flush_res = bm_->flush_log();
  CHFS_ASSERT(flush_res.is_ok(), "Failed to flush log");

  if (is_checkpoint_enabled_ && get_log_entry_num() >= kMaxLogSize) {
    checkpoint();
    this->begin_txn_id = txn_id + 1;
  }
}

// {Your code here}
auto CommitLog::checkpoint() -> void {
  std::vector<LogEntry> new_log_data;
  for (int i = 0; i < log_entry_num; i++) {
    if (log_data[i].block_id != KInvalidBlockID) {
      new_log_data.push_back(log_data[i]);
    }
  }

  memset(log_data, 0, log_entry_num * sizeof(LogEntry));
  std::copy(new_log_data.begin(), new_log_data.end(), log_data);
  log_entry_num = new_log_data.size();

  auto flush_res = bm_->flush_log();
  CHFS_ASSERT(flush_res.is_ok(), "Failed to flush log");
}

// {Your code here}
auto CommitLog::recover() -> void {
  std::unique_lock<std::mutex> lock(this->mutex_);

  for (int i = 0; i < log_entry_num; i++) {
    if (log_data[i].block_id == KInvalidBlockID)
      continue;
    auto write_res =
        bm_->write_block(log_data[i].block_id, log_data[i].block_state);
    CHFS_ASSERT(write_res.is_ok(), "Failed to recover block");
  }
}
}; // namespace chfs