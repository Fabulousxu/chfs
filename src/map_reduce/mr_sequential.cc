#include <string>
#include <utility>
#include <vector>
#include <algorithm>
#include <map>
#include <chrono>
#include <thread>

#include "map_reduce/protocol.h"

namespace mapReduce {
    SequentialMapReduce::SequentialMapReduce(std::shared_ptr<chfs::ChfsClient> client,
                                             const std::vector<std::string> &files_, std::string resultFile) {
        chfs_client = std::move(client);
        files = files_;
        outPutFile = resultFile;
    }

    void SequentialMapReduce::doWork() {
        std::vector<KeyVal> maps;
        std::vector<std::pair<std::string, std::vector<std::string>>> words;
        std::vector<KeyVal> reduces;
        std::string content;

        for (const auto &file : files) {
            auto lookup_res = chfs_client->lookup(1, file);
            if (lookup_res.is_err())
                continue;
            auto inode_id = lookup_res.unwrap();

            auto type_attr_res = chfs_client->get_type_attr(inode_id);
            if (type_attr_res.is_err())
                continue;
            auto [type, attr] = type_attr_res.unwrap();

            auto read_res = chfs_client->read_file(inode_id, 0, attr.size);
            if (read_res.is_err())
                continue;
            auto content = read_res.unwrap();

            auto map_res = Map({content.begin(), content.end()});
            maps.insert(maps.end(), map_res.begin(), map_res.end());
        }

        for (auto it = maps.begin(); it != maps.end(); it++) {
            bool exist = false;
            for (auto &[key, val] : words) {
                if (it->key == key) {
                    exist = true;
                    val.push_back(it->val);
                    break;
                }
            }
            if (!exist)
                words.push_back({it->key, {it->val}});
        }

        for (const auto &[key, val] : words)
            reduces.push_back({key, Reduce(key, val)});

        std::sort(reduces.begin(), reduces.end(), [](const KeyVal &lhs, const KeyVal &rhs) {
            return lhs.key < rhs.key;
        });

        for (const auto &[key, val] : reduces)
            content += key + " " + val + " ";

        auto lookup_res = chfs_client->lookup(1, outPutFile);
        chfs::inode_id_t inode_id;
        if (lookup_res.is_err()) {
            auto mkdir_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, outPutFile);
            if (mkdir_res.is_err())
                return;
            inode_id = mkdir_res.unwrap();
        } else 
            inode_id = lookup_res.unwrap();
        
        chfs_client->write_file(inode_id, 0, {content.begin(), content.end()});
    }
}