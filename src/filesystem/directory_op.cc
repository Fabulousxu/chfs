#include <algorithm>
#include <sstream>

#include "filesystem/directory_op.h"

namespace chfs {

/**
 * Some helper functions
 */
auto string_to_inode_id(std::string &data) -> inode_id_t {
  std::stringstream ss(data);
  inode_id_t inode;
  ss >> inode;
  return inode;
}

auto inode_id_to_string(inode_id_t id) -> std::string {
  std::stringstream ss;
  ss << id;
  return ss.str();
}

// {Your code here}
auto dir_list_to_string(const std::list<DirectoryEntry> &entries)
    -> std::string {
  std::ostringstream oss;
  usize cnt = 0;
  for (const auto &entry : entries) {
    oss << entry.name << ':' << entry.id;
    if (cnt < entries.size() - 1) {
      oss << '/';
    }
    cnt += 1;
  }
  return oss.str();
}

// {Your code here}
auto append_to_directory(std::string src, std::string filename, inode_id_t id)
    -> std::string {

  // Append the new directory entry to `src`.
  if (!src.empty())
    src += '/';

  src += filename + ':' + inode_id_to_string(id);
  
  return src;
}

// {Your code here}
void parse_directory(std::string &src, std::list<DirectoryEntry> &list) {

  std::stringstream ss(src);
  std::string entry;

  while (std::getline(ss, entry, '/')) {
    auto pos = entry.rfind(':');
    CHFS_ASSERT(pos != std::string::npos, "Invalid directory entry");
    auto name = entry.substr(0, pos);
    auto id_str = entry.substr(pos + 1);
    auto id = string_to_inode_id(id_str);
    list.push_back({name, id});
  }

}

// {Your code here}
auto rm_from_directory(std::string src, std::string filename) -> std::string {

  auto res = std::string("");

  // Remove the directory entry from `src`.
  std::list<DirectoryEntry> list;
  parse_directory(src, list);

  list.remove_if([filename](const DirectoryEntry &entry) {
    return entry.name == filename;
  });

  res = dir_list_to_string(list);

  return res;
}

/**
 * { Your implementation here }
 */
auto read_directory(FileOperation *fs, inode_id_t id,
                    std::list<DirectoryEntry> &list) -> ChfsNullResult {
  
  auto res = fs->read_file(id);
  if (res.is_err()) {
    return ChfsNullResult(res.unwrap_error());
  }

  auto dir_u8v = res.unwrap();
  std::string dir_str(dir_u8v.begin(), dir_u8v.end());
  parse_directory(dir_str, list);

  return KNullOk;
}

// {Your code here}
auto FileOperation::lookup(inode_id_t id, const char *name)
    -> ChfsResult<inode_id_t> {
  std::list<DirectoryEntry> list;

  auto res = read_directory(this, id, list);
  if (res.is_err()) {
    return ChfsResult<inode_id_t>(res.unwrap_error());
  }

  for (const auto &entry : list) {
    if (entry.name == name) {
      return ChfsResult<inode_id_t>(entry.id);
    }
  }

  return ChfsResult<inode_id_t>(ErrorType::NotExist);
}

// {Your code here}
auto FileOperation::mk_helper(inode_id_t id, const char *name, InodeType type)
    -> ChfsResult<inode_id_t> {

  // Check if `name` already exists in the parent.
  // If already exist, return ErrorType::AlreadyExist.
  auto lookup_res = lookup(id, name);
  if (lookup_res.is_ok()) {
    return ChfsResult<inode_id_t>(ErrorType::AlreadyExist);
  }
  
  // Create the new inode.
  auto inode_res = alloc_inode(type);
  if (inode_res.is_err()) {
    return ChfsResult<inode_id_t>(inode_res.unwrap_error());
  }
  auto inode_id = inode_res.unwrap();
  
  // Append the new entry to the parent directory.
  std::list<DirectoryEntry> list;
  auto read_res = read_directory(this, id, list);
  if (read_res.is_err()) {
    return ChfsResult<inode_id_t>(read_res.unwrap_error());
  }
  auto dir_str = dir_list_to_string(list);
  auto new_dir_str = append_to_directory(dir_str, name, inode_id);

  std::vector<u8> content(new_dir_str.begin(), new_dir_str.end());
  auto write_res = write_file(id, content);
  if (write_res.is_err()) {
    return ChfsResult<inode_id_t>(write_res.unwrap_error());
  }

  return ChfsResult<inode_id_t>(static_cast<inode_id_t>(inode_id));
}

// {Your code here}
auto FileOperation::unlink(inode_id_t parent, const char *name)
    -> ChfsNullResult {

  // Remove the file, you can use the function `remove_file`
  auto lookup_res = lookup(parent, name);
  if (lookup_res.is_err()) {
    return ChfsNullResult(lookup_res.unwrap_error());
  }
  auto inode_id = lookup_res.unwrap();

  auto remove_res = remove_file(inode_id);
  if (remove_res.is_err()) {
    return ChfsNullResult(remove_res.unwrap_error());
  }
  
  // Remove the entry from the directory.
  std::list<DirectoryEntry> list;
  auto read_res = read_directory(this, parent, list);
  if (read_res.is_err()) {
    return ChfsNullResult(read_res.unwrap_error());
  }
  auto dir_str = dir_list_to_string(list);
  auto new_dir_str = rm_from_directory(dir_str, name);

  std::vector<u8> content(new_dir_str.begin(), new_dir_str.end());
  auto write_res = write_file(parent, content);
  if (write_res.is_err()) {
    return ChfsNullResult(write_res.unwrap_error());
  }
  
  return KNullOk;
}

} // namespace chfs
