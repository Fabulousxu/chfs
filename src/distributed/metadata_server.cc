#include "distributed/metadata_server.h"
#include "common/util.h"
#include "filesystem/directory_op.h"
#include <fstream>

namespace chfs {

inline auto MetadataServer::bind_handlers() {
  server_->bind("mknode",
                [this](u8 type, inode_id_t parent, std::string const &name) {
                  return this->mknode(type, parent, name);
                });
  server_->bind("unlink", [this](inode_id_t parent, std::string const &name) {
    return this->unlink(parent, name);
  });
  server_->bind("lookup", [this](inode_id_t parent, std::string const &name) {
    return this->lookup(parent, name);
  });
  server_->bind("get_block_map",
                [this](inode_id_t id) { return this->get_block_map(id); });
  server_->bind("alloc_block",
                [this](inode_id_t id) { return this->allocate_block(id); });
  server_->bind("free_block",
                [this](inode_id_t id, block_id_t block, mac_id_t machine_id) {
                  return this->free_block(id, block, machine_id);
                });
  server_->bind("readdir", [this](inode_id_t id) { return this->readdir(id); });
  server_->bind("get_type_attr",
                [this](inode_id_t id) { return this->get_type_attr(id); });
}

inline auto MetadataServer::init_fs(const std::string &data_path) {
  /**
   * Check whether the metadata exists or not.
   * If exists, we wouldn't create one from scratch.
   */
  bool is_initialed = is_file_exist(data_path);

  auto block_manager = std::shared_ptr<BlockManager>(nullptr);
  if (is_log_enabled_) {
    block_manager =
        std::make_shared<BlockManager>(data_path, KDefaultBlockCnt, true);
  } else {
    block_manager = std::make_shared<BlockManager>(data_path, KDefaultBlockCnt);
  }

  CHFS_ASSERT(block_manager != nullptr, "Cannot create block manager.");

  if (is_initialed) {
    auto origin_res = FileOperation::create_from_raw(block_manager);
    std::cout << "Restarting..." << std::endl;
    if (origin_res.is_err()) {
      std::cerr << "Original FS is bad, please remove files manually."
                << std::endl;
      exit(1);
    }

    operation_ = origin_res.unwrap();
  } else {
    operation_ = std::make_shared<FileOperation>(block_manager,
                                                 DistributedMaxInodeSupported);
    std::cout << "We should init one new FS..." << std::endl;
    /**
     * If the filesystem on metadata server is not initialized, create
     * a root directory.
     */
    auto init_res = operation_->alloc_inode(InodeType::Directory);
    if (init_res.is_err()) {
      std::cerr << "Cannot allocate inode for root directory." << std::endl;
      exit(1);
    }

    CHFS_ASSERT(init_res.unwrap() == 1, "Bad initialization on root dir.");
  }

  running = false;
  num_data_servers =
      0; // Default no data server. Need to call `reg_server` to add.

  if (is_log_enabled_) {
    if (may_failed_)
      operation_->block_manager_->set_may_fail(true);
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled_);
  }

  bind_handlers();

  /**
   * The metadata server wouldn't start immediately after construction.
   * It should be launched after all the data servers are registered.
   */
}

MetadataServer::MetadataServer(u16 port, const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

MetadataServer::MetadataServer(std::string const &address, u16 port,
                               const std::string &data_path,
                               bool is_log_enabled, bool is_checkpoint_enabled,
                               bool may_failed)
    : is_log_enabled_(is_log_enabled), may_failed_(may_failed),
      is_checkpoint_enabled_(is_checkpoint_enabled) {
  server_ = std::make_unique<RpcServer>(address, port);
  init_fs(data_path);
  if (is_log_enabled_) {
    commit_log = std::make_shared<CommitLog>(operation_->block_manager_,
                                             is_checkpoint_enabled);
  }
}

// {Your code here}
auto MetadataServer::mknode(u8 type, inode_id_t parent, const std::string &name)
    -> inode_id_t {
  std::unique_lock<std::mutex> lock(mutex_);

  auto inode_type = type == DirectoryType     ? InodeType::Directory
                    : type == RegularFileType ? InodeType::FILE
                                              : InodeType::Unknown;
  if (inode_type == InodeType::Unknown)
    return KInvalidInodeID;

  if (is_log_enabled_) {
    operation_->block_manager_->set_write_to_log(true);
  }

  auto res = operation_->mk_helper(parent, name.c_str(), inode_type);
  if (res.is_err())
    return KInvalidInodeID;

  if (!is_log_enabled_) {
    return res.unwrap();
  }

  auto log_ops = operation_->block_manager_->set_write_to_log(false);
  auto txn_id = commit_log->gen_txn_id();
  commit_log->append_log(txn_id, log_ops);
  for (auto &op : log_ops) {
    auto write_res = operation_->block_manager_->write_block(
        op->block_id_, op->new_block_state_.data());
    if (write_res.is_err())
      return KInvalidInodeID;
  }
  commit_log->commit_log(txn_id);

  return res.unwrap();
}

// {Your code here}
auto MetadataServer::unlink(inode_id_t parent, const std::string &name)
    -> bool {
  std::unique_lock<std::mutex> lock(mutex_);

  if (is_log_enabled_) {
    operation_->block_manager_->set_write_to_log(true);
  }

  auto lookup_res = operation_->lookup(parent, name.c_str());
  if (lookup_res.is_err())
    return false;
  auto inode_id = lookup_res.unwrap();

  auto type_res = operation_->inode_manager_->get_type(inode_id);
  if (type_res.is_err())
    return false;
  auto type = type_res.unwrap();

  if (type == InodeType::Directory) {
    auto unlink_res = operation_->unlink(parent, name.c_str());
    if (unlink_res.is_err())
      return false;

    if (!is_log_enabled_) {
      return true;
    }

    auto log_ops = operation_->block_manager_->set_write_to_log(false);
    auto txn_id = commit_log->gen_txn_id();
    commit_log->append_log(txn_id, log_ops);
    for (auto &op : log_ops) {
      auto write_res = operation_->block_manager_->write_block(
          op->block_id_, op->new_block_state_.data());
      if (write_res.is_err())
        return false;
    }
    commit_log->commit_log(txn_id);

    return true;
  } else if (type == InodeType::FILE) {
    auto block_map = get_block_map(inode_id);

    auto inode_res = operation_->inode_manager_->get(inode_id);
    if (inode_res.is_err())
       return false;
    auto inode_block_id = inode_res.unwrap();
    auto free_inode_res = operation_->inode_manager_->free_inode(inode_id);
    if (free_inode_res.is_err())
      return false;
    auto free_block_res =
        operation_->block_allocator_->deallocate(inode_block_id);
    if (free_block_res.is_err())
      return false;
    
    for (auto &block : block_map) {
      auto [block_id, mac_id, version_id] = block;
      auto it = clients_.find(mac_id);
      if (it == clients_.end())
        return false;
      auto cli = it->second;

      auto free_res = cli->call("free_block", block_id);
      if (free_res.is_err())
        return false;
      
      auto ok = free_res.unwrap()->as<bool>();
      if (!ok)
        return false;
    }
  }
  
  std::list<DirectoryEntry> list;
  auto read_res = read_directory(operation_.get(), parent, list);
  if (read_res.is_err())
    return false;
  auto dir_str = dir_list_to_string(list);
  auto new_dir_str = rm_from_directory(dir_str, name);

  std::vector<u8> content(new_dir_str.begin(), new_dir_str.end());
  auto write_res = operation_->write_file(parent, content);
  if (write_res.is_err())
    return false;

  if (!is_log_enabled_) {
    return true;
  }

  auto log_ops = operation_->block_manager_->set_write_to_log(false);
  auto txn_id = commit_log->gen_txn_id();
  commit_log->append_log(txn_id, log_ops);
  for (auto &op : log_ops) {
    auto write_res = operation_->block_manager_->write_block(
        op->block_id_, op->new_block_state_.data());
    if (write_res.is_err())
      return false;
  }
  commit_log->commit_log(txn_id);
  
  return true;
}

// {Your code here}
auto MetadataServer::lookup(inode_id_t parent, const std::string &name)
    -> inode_id_t {
  std::unique_lock<std::mutex> lock(mutex_);

  auto res = operation_->lookup(parent, name.c_str());
  if (res.is_err())
    return KInvalidInodeID;
  return res.unwrap();
}

// {Your code here}
auto MetadataServer::get_block_map(inode_id_t id) -> std::vector<BlockInfo> {
  if (id > operation_->inode_manager_->get_max_inode_supported())
    return {};
  if (id == KInvalidInodeID)
    return {};

  const auto block_size = operation_->block_manager_->block_size();
  const auto version_per_block = block_size / sizeof(block_id_t);

  auto inode_res = operation_->inode_manager_->get(id);
  if (inode_res.is_err())
    return {};
  auto inode_block_id = inode_res.unwrap();

  std::vector<u8> buffer(block_size);
  auto inode_p = reinterpret_cast<Inode *>(buffer.data());
  auto inode_read_res =
      operation_->block_manager_->read_block(inode_block_id, buffer.data());
  if (inode_read_res.is_err())
    return {};
  if (inode_p->get_type() != InodeType::FILE)
    return {};
  
  std::vector<BlockInfo> block_info;
  for (int i = 0; i < inode_p->get_nblocks(); i += 2) {
    auto block_id = inode_p->blocks[i];
    if (block_id == KInvalidBlockID)
      break;

    auto mac_id = static_cast<mac_id_t>(inode_p->blocks[i + 1]);
    auto it = clients_.find(mac_id);
    if (it == clients_.end())
      return {};
    auto cli = it->second;

    auto version_block_id = block_id / version_per_block;
    auto version_block_offset = block_id % version_per_block;

    auto version_res = cli->call("read_data", version_block_id,
                                 version_block_offset * sizeof(version_t),
                                 sizeof(version_t), 0);
    if (version_res.is_err())
      return {};
    auto version_u8v = version_res.unwrap()->as<std::vector<u8>>();
    auto version_p = reinterpret_cast<version_t *>(version_u8v.data());
    auto version = *version_p;

    block_info.push_back({block_id, mac_id, version});
  }

  return block_info;
}

// {Your code here}
auto MetadataServer::allocate_block(inode_id_t id) -> BlockInfo {
  std::unique_lock<std::mutex> lock(mutex_);

  if (id > operation_->inode_manager_->get_max_inode_supported())
    return {KInvalidBlockID, 0, 0};
  if (id == KInvalidInodeID)
    return {KInvalidBlockID, 0, 0};

  auto inode_res = operation_->inode_manager_->get(id);
  if (inode_res.is_err())
    return {KInvalidBlockID, 0, 0};
  auto inode_block_id = inode_res.unwrap();
  if (inode_block_id == KInvalidBlockID)
    return {KInvalidBlockID, 0, 0};

  const auto block_size = operation_->block_manager_->block_size();
  std::vector<u8> inode(block_size);
  auto inode_p = reinterpret_cast<Inode *>(inode.data());
  auto inode_read_res =
      operation_->block_manager_->read_block(inode_block_id, inode.data());
  if (inode_read_res.is_err())
    return {KInvalidBlockID, 0, 0};
  if (inode_p->get_type() != InodeType::FILE)
    return {KInvalidBlockID, 0, 0};

  int block_idx = 0;
  while (block_idx < inode_p->get_nblocks() &&
         inode_p->blocks[block_idx] != KInvalidBlockID)
    block_idx += 2;
  if (block_idx >= inode_p->get_nblocks())
    return {KInvalidBlockID, 0, 0};

  auto rand_mac_idx = generator.rand(0, clients_.size() - 1);
  auto it = clients_.begin();
  std::advance(it, rand_mac_idx);
  auto [mac_id, cli] = *it;

  auto alloc_res = cli->call("alloc_block");
  if (alloc_res.is_err())
    return {KInvalidBlockID, 0, 0};
  auto [block_id, version] =
      alloc_res.unwrap()->as<std::pair<block_id_t, version_t>>();
  if (block_id == KInvalidBlockID)
    return {KInvalidBlockID, 0, 0};
  
  inode_p->set_block_direct(block_idx, block_id);
  inode_p->set_block_direct(block_idx + 1, mac_id);
  inode_p->inner_attr.size += block_size;

  auto write_res =
      operation_->block_manager_->write_block(inode_block_id, inode.data());
  if (write_res.is_err())
    return {KInvalidBlockID, 0, 0};

  return {block_id, mac_id, version};
}

// {Your code here}
auto MetadataServer::free_block(inode_id_t id, block_id_t block_id,
                                mac_id_t machine_id) -> bool {
  std::unique_lock<std::mutex> lock(mutex_);

  if (id > operation_->inode_manager_->get_max_inode_supported())
    return false;
  if (id == KInvalidInodeID)
    return false;
  if (block_id == KInvalidBlockID)
    return false;
  
  auto inode_res = operation_->inode_manager_->get(id);
  if (inode_res.is_err())
    return false;
  auto inode_block_id = inode_res.unwrap();

  const auto block_size = operation_->block_manager_->block_size();
  std::vector<u8> buffer(block_size);
  auto inode_p = reinterpret_cast<Inode *>(buffer.data());
  auto inode_read_res =
      operation_->block_manager_->read_block(inode_block_id, buffer.data());
  if (inode_read_res.is_err())
    return false;
  if (inode_p->get_type() != InodeType::FILE)
    return false;
  
  int dir_block_idx = 0;
  while (dir_block_idx < inode_p->get_nblocks() &&
         inode_p->blocks[dir_block_idx] != block_id) {
    if (inode_p->blocks[dir_block_idx] == block_id && 
        inode_p->blocks[dir_block_idx + 1] == machine_id)
      break;
    dir_block_idx += 2;
  }
  if (dir_block_idx >= inode_p->get_nblocks())
    return false;
  
  auto it = clients_.find(machine_id);
  if (it == clients_.end())
    return false;
  auto cli = it->second;

  auto free_res = cli->call("free_block", block_id);
  if (free_res.is_err())
    return false;
  auto ok = free_res.unwrap()->as<bool>();
  if (!ok)
    return false;

  for (; dir_block_idx < inode_p->get_nblocks() - 2; dir_block_idx += 2) {
    inode_p->set_block_direct(dir_block_idx,
                              inode_p->blocks[dir_block_idx + 2]);
    inode_p->set_block_direct(dir_block_idx + 1,
                              inode_p->blocks[dir_block_idx + 3]);
  }
  inode_p->set_block_direct(dir_block_idx, KInvalidBlockID);
  inode_p->set_block_direct(dir_block_idx + 1, 0);
  if (inode_p->get_size() > block_size)
    inode_p->inner_attr.size -= block_size;
  else
    inode_p->inner_attr.size = 0; 

  auto write_res =
      operation_->block_manager_->write_block(inode_block_id, buffer.data());
  if (write_res.is_err())
    return false;

  return true;
}

// {Your code here}
auto MetadataServer::readdir(inode_id_t node)
    -> std::vector<std::pair<std::string, inode_id_t>> {
  std::list<DirectoryEntry> list;
  auto read_res = read_directory(operation_.get(), node, list);
  if (read_res.is_err())
    return {};

  std::vector<std::pair<std::string, inode_id_t>> dir_list;
  for (const auto &entry : list)
    dir_list.push_back({entry.name, entry.id});
  return dir_list;
}

// {Your code here}
auto MetadataServer::get_type_attr(inode_id_t id)
    -> std::tuple<u64, u64, u64, u64, u8> {
  auto res = operation_->get_type_attr(id);
  if (res.is_err())
    return {0, 0, 0, 0, 0};
  auto [type, attr] = res.unwrap();
  auto type_u8 = type == InodeType::FILE        ? RegularFileType
                 : type == InodeType::Directory ? DirectoryType
                                                : 0;
  return {attr.size, attr.atime, attr.mtime, attr.ctime, type_u8};
}

auto MetadataServer::reg_server(const std::string &address, u16 port,
                                bool reliable) -> bool {
  num_data_servers += 1;
  auto cli = std::make_shared<RpcClient>(address, port, reliable);
  clients_.insert(std::make_pair(num_data_servers, cli));

  return true;
}

auto MetadataServer::run() -> bool {
  if (running)
    return false;

  // Currently we only support async start
  server_->run(true, num_worker_threads);
  running = true;
  return true;
}

} // namespace chfs