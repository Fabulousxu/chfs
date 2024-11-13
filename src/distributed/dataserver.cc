#include "distributed/dataserver.h"
#include "common/util.h"

namespace chfs {

auto DataServer::initialize(std::string const &data_path) {
  /**
   * At first check whether the file exists or not.
   * If so, which means the distributed chfs has
   * already been initialized and can be rebuilt from
   * existing data.
   */
  bool is_initialized = is_file_exist(data_path);

  auto bm = std::shared_ptr<BlockManager>(
      new BlockManager(data_path, KDefaultBlockCnt));

  const auto version_per_block = bm->block_size() / sizeof(version_t);
  auto n_version_blocks = KDefaultBlockCnt / version_per_block;
  if (n_version_blocks * version_per_block < KDefaultBlockCnt) {
    n_version_blocks += 1;
  }

  if (is_initialized) {
    block_allocator_ =
        std::make_shared<BlockAllocator>(bm, n_version_blocks, false);
  } else {
    // We need to reserve some blocks for storing the version of each block
    block_allocator_ = std::shared_ptr<BlockAllocator>(
        new BlockAllocator(bm, n_version_blocks, true));

    for (int i = 0; i < n_version_blocks; i++)
      block_allocator_->bm->zero_block(i);
  }

  // Initialize the RPC server and bind all handlers
  server_->bind("read_data", [this](block_id_t block_id, usize offset,
                                    usize len, version_t version) {
    return this->read_data(block_id, offset, len, version);
  });
  server_->bind("write_data", [this](block_id_t block_id, usize offset,
                                     std::vector<u8> &buffer) {
    return this->write_data(block_id, offset, buffer);
  });
  server_->bind("alloc_block", [this]() { return this->alloc_block(); });
  server_->bind("free_block", [this](block_id_t block_id) {
    return this->free_block(block_id);
  });

  // Launch the rpc server to listen for requests
  server_->run(true, num_worker_threads);
}

DataServer::DataServer(u16 port, const std::string &data_path)
    : server_(std::make_unique<RpcServer>(port)) {
  initialize(data_path);
}

DataServer::DataServer(std::string const &address, u16 port,
                       const std::string &data_path)
    : server_(std::make_unique<RpcServer>(address, port)) {
  initialize(data_path);
}

DataServer::~DataServer() { server_.reset(); }

// {Your code here}
auto DataServer::read_data(block_id_t block_id, usize offset, usize len,
                           version_t version) -> std::vector<u8> {
  if (block_id >= block_allocator_->bm->total_blocks())
    return {};
  if (offset + len > block_allocator_->bm->block_size())
    return {};

  const auto block_size = block_allocator_->bm->block_size();
  const auto version_per_block = block_size / sizeof(version_t);
  auto version_block_id = block_id / version_per_block;
  auto version_block_offset = block_id % version_per_block;

  std::vector<u8> buffer(block_size);
  auto block_res =
      block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (block_res.is_err())
    return {};

  auto version_p = reinterpret_cast<version_t *>(buffer.data());
  if (version_p[version_block_offset] != version)
    return {};

  auto version_res = block_allocator_->bm->read_block(block_id, buffer.data());
  if (version_res.is_err())
    return {};
  return std::vector<u8>(buffer.begin() + offset,
                         buffer.begin() + offset + len);
}

// {Your code here}
auto DataServer::write_data(block_id_t block_id, usize offset,
                            std::vector<u8> &buffer) -> bool {
  auto res = block_allocator_->bm->write_partial_block(
      block_id, buffer.data(), offset, buffer.size());
  return res.is_ok();
}

// {Your code here}
auto DataServer::alloc_block() -> std::pair<block_id_t, version_t> {
  auto res = block_allocator_->allocate();
  if (res.is_err())
    return {0, 0};
  auto block_id = res.unwrap();

  const auto block_size = block_allocator_->bm->block_size();
  const auto version_per_block = block_size / sizeof(version_t);
  auto version_block_id = block_id / version_per_block;
  auto version_block_offset = block_id % version_per_block;

  std::vector<u8> buffer(block_size);
  auto block_res =
      block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (block_res.is_err())
    return {block_id, 0};

  auto version_p = reinterpret_cast<version_t *>(buffer.data());
  auto new_version = version_p[version_block_offset] + 1;
  version_p[version_block_offset] = new_version;
  auto version_res = block_allocator_->bm->write_partial_block(
      version_block_id,
      reinterpret_cast<u8 *>(&version_p[version_block_offset]),
      version_block_offset * sizeof(version_t), sizeof(version_t));
  if (version_res.is_err())
    return {block_id, new_version - 1};
  return {block_id, new_version};
}

// {Your code here}
auto DataServer::free_block(block_id_t block_id) -> bool {
  auto res = block_allocator_->deallocate(block_id);
  if (res.is_err())
    return false;
  
  const auto block_size = block_allocator_->bm->block_size();
  const auto version_per_block = block_size / sizeof(version_t);
  auto version_block_id = block_id / version_per_block;
  auto version_block_offset = block_id % version_per_block;

  std::vector<u8> buffer(block_size);
  auto block_res =
      block_allocator_->bm->read_block(version_block_id, buffer.data());
  if (block_res.is_err())
    return false;
  
  auto version_p = reinterpret_cast<version_t *>(buffer.data());
  auto new_version = version_p[version_block_offset] + 1;
  version_p[version_block_offset] = new_version;
  auto version_res = block_allocator_->bm->write_partial_block(
      version_block_id,
      reinterpret_cast<u8 *>(&version_p[version_block_offset]),
      version_block_offset * sizeof(version_t), sizeof(version_t));
  if (version_res.is_err())
    return false;
  return true;
}
} // namespace chfs