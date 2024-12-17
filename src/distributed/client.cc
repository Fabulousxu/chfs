#include "distributed/client.h"
#include "common/macros.h"
#include "common/util.h"
#include "distributed/metadata_server.h"

namespace chfs {

ChfsClient::ChfsClient() : num_data_servers(0) {}

auto ChfsClient::reg_server(ServerType type, const std::string &address,
                            u16 port, bool reliable) -> ChfsNullResult {
  switch (type) {
  case ServerType::DATA_SERVER:
    num_data_servers += 1;
    data_servers_.insert({num_data_servers, std::make_shared<RpcClient>(
                                                address, port, reliable)});
    break;
  case ServerType::METADATA_SERVER:
    metadata_server_ = std::make_shared<RpcClient>(address, port, reliable);
    break;
  default:
    std::cerr << "Unknown Type" << std::endl;
    exit(1);
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::mknode(FileType type, inode_id_t parent,
                        const std::string &name) -> ChfsResult<inode_id_t> {
  auto metadata_res =
      metadata_server_->call("mknode", static_cast<u8>(type), parent, name);
  if (metadata_res.is_err())
    return metadata_res.unwrap_error();
  auto inode_id = metadata_res.unwrap()->as<inode_id_t>();
  if (inode_id == KInvalidInodeID)
    return ErrorType::DONE;
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::unlink(inode_id_t parent, std::string const &name)
    -> ChfsNullResult {
  auto unlink_res = metadata_server_->call("unlink", parent, name);
  if (unlink_res.is_err())
    return unlink_res.unwrap_error();
  auto ok = unlink_res.unwrap()->as<bool>();
  if (!ok)
    return ErrorType::DONE;
  return KNullOk;
}

// {Your code here}
auto ChfsClient::lookup(inode_id_t parent, const std::string &name)
    -> ChfsResult<inode_id_t> {
  auto lookup_res = metadata_server_->call("lookup", parent, name);
  if (lookup_res.is_err())
    return lookup_res.unwrap_error();
  auto inode_id = lookup_res.unwrap()->as<inode_id_t>();
  return ChfsResult<inode_id_t>(inode_id);
}

// {Your code here}
auto ChfsClient::readdir(inode_id_t id)
    -> ChfsResult<std::vector<std::pair<std::string, inode_id_t>>> {
  auto readdir_res = metadata_server_->call("readdir", id);
  if (readdir_res.is_err())
    return readdir_res.unwrap_error();
  using ret_type = std::vector<std::pair<std::string, inode_id_t>>;
  return ChfsResult<ret_type>(readdir_res.unwrap()->as<ret_type>());
}

// {Your code here}
auto ChfsClient::get_type_attr(inode_id_t id)
    -> ChfsResult<std::pair<InodeType, FileAttr>> {
  auto get_type_attr_res = metadata_server_->call("get_type_attr", id);
  if (get_type_attr_res.is_err())
    return get_type_attr_res.unwrap_error();
  auto [size, atime, mtime, ctime, type] =
      get_type_attr_res.unwrap()->as<std::tuple<u64, u64, u64, u64, u8>>();
  auto inode_type = type == DirectoryType     ? InodeType::Directory
                    : type == RegularFileType ? InodeType::FILE
                                              : InodeType::Unknown;
  return ChfsResult<std::pair<InodeType, FileAttr>>(
      {inode_type, FileAttr{atime, mtime, ctime, size}});
}

/**
 * Read and Write operations are more complicated.
 */
// {Your code here}
auto ChfsClient::read_file(inode_id_t id, usize offset, usize size)
    -> ChfsResult<std::vector<u8>> {
  const auto block_size = DiskBlockSize;
  auto begin_block_idx = offset / block_size;
  auto begin_block_offset = offset % block_size;
  auto end_block_idx = (offset + size) / block_size;
  auto end_block_offset = (offset + size) % block_size;
  if (end_block_offset == 0) {
    end_block_idx--;
    end_block_offset = block_size;
  }

  auto block_map_res = metadata_server_->call("get_block_map", id);
  if (block_map_res.is_err())
    return block_map_res.unwrap_error();
  auto block_map = block_map_res.unwrap()->as<std::vector<BlockInfo>>();
  if (end_block_idx >= block_map.size())
    return ErrorType::INVALID_ARG;

  std::vector<u8> content;
  for (auto block_map_it = block_map.begin() + begin_block_idx;
       block_map_it != block_map.begin() + end_block_idx + 1; block_map_it++) {
    auto [block_id, mac_id, version] = *block_map_it;

    auto it = data_servers_.find(mac_id);
    if (it == data_servers_.end())
      return ErrorType::DONE;
    auto cli = it->second;

    auto read_offset = block_map_it == block_map.begin() + begin_block_idx
                           ? begin_block_offset
                           : 0;
    auto read_end = block_map_it == block_map.begin() + end_block_idx
                        ? end_block_offset
                        : block_size;
    auto read_size = read_end - read_offset;

    auto read_res =
        cli->call("read_data", block_id, read_offset, read_size, version);
    if (read_res.is_err())
      return read_res.unwrap_error();
    auto block_content = read_res.unwrap()->as<std::vector<u8>>();
    if (block_content.size() != read_size)
      return ErrorType::DONE;
    content.insert(content.end(), block_content.begin(), block_content.end());
  }
  if (content.size() != size)
    return ErrorType::DONE;

  return ChfsResult<std::vector<u8>>(content);
}

// {Your code here}
auto ChfsClient::write_file(inode_id_t id, usize offset, std::vector<u8> data)
    -> ChfsNullResult {
  const auto block_size = DiskBlockSize;
  auto size = data.size();
  auto begin_block_idx = offset / block_size;
  auto begin_block_offset = offset % block_size;
  auto end_block_idx = (offset + size) / block_size;
  auto end_block_offset = (offset + size) % block_size;
  if (end_block_offset == 0) {
    end_block_idx--;
    end_block_offset = block_size;
  }

  auto block_map_res = metadata_server_->call("get_block_map", id);
  if (block_map_res.is_err())
    return block_map_res.unwrap_error();
  auto block_map = block_map_res.unwrap()->as<std::vector<BlockInfo>>();

  for (int i = block_map.size(); i <= end_block_idx; i++) {
    auto allocate_res = metadata_server_->call("alloc_block", id);
    if (allocate_res.is_err())
      return allocate_res.unwrap_error();
    auto block_info = allocate_res.unwrap()->as<BlockInfo>();
    auto [block_id, mac_id, version] = block_info;
    if (block_id == KInvalidBlockID) return ErrorType::DONE;
    block_map.push_back(block_info);
  }

  usize data_offset = 0;
  for (auto block_map_it = block_map.begin() + begin_block_idx;
       block_map_it != block_map.begin() + end_block_idx + 1; block_map_it++) {
    auto [block_id, mac_id, version] = *block_map_it;

    auto it = data_servers_.find(mac_id);
    if (it == data_servers_.end())
      return ErrorType::DONE;
    auto cli = it->second;

    auto write_offset = block_map_it == block_map.begin() + begin_block_idx
                            ? begin_block_offset
                            : 0;
    auto write_end = block_map_it == block_map.begin() + end_block_idx
                         ? end_block_offset
                         : block_size;
    auto write_size = write_end - write_offset;

    auto write_buffer = std::vector<u8>(
        data.begin() + data_offset, data.begin() + data_offset + write_size);
    auto write_res =
        cli->call("write_data", block_id, write_offset, write_buffer);
    if (write_res.is_err())
      return write_res.unwrap_error();
    auto ok = write_res.unwrap()->as<bool>();
    if (!ok)
      return ErrorType::DONE;
    data_offset += write_size;
  }

  return KNullOk;
}

// {Your code here}
auto ChfsClient::free_file_block(inode_id_t id, block_id_t block_id,
                                 mac_id_t mac_id) -> ChfsNullResult {
  auto free_block_res =
      metadata_server_->call("free_block", id, block_id, mac_id);
  if (free_block_res.is_err())
    return free_block_res.unwrap_error();
  auto ok = free_block_res.unwrap()->as<bool>();
  if (!ok)
    return ErrorType::DONE;
  return KNullOk;
}

} // namespace chfs