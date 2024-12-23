#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <mutex>
#include <string>
#include <vector>
#include <map>
#include <chrono>
#include <thread>

#include "map_reduce/protocol.h"

namespace mapReduce {

    Worker::Worker(MR_CoordinatorConfig config) {
        mr_client = std::make_unique<chfs::RpcClient>(config.ip_address, config.port, true);
        outPutFile = config.resultFile;
        chfs_client = config.client;
        work_thread = std::make_unique<std::thread>(&Worker::doWork, this);
    }

    void Worker::doMap(int index, const std::string &filename) {
        auto loopup_res = chfs_client->lookup(1, filename);
        if (loopup_res.is_err())
            return;
        auto inode_id = loopup_res.unwrap();

        auto type_attr_res = chfs_client->get_type_attr(inode_id);
        if (type_attr_res.is_err())
            return;
        auto [type, attr] = type_attr_res.unwrap();

        auto read_res = chfs_client->read_file(inode_id, 0, attr.size);
        if (read_res.is_err())
            return;
        auto read_content = read_res.unwrap();

        std::string content(read_content.begin(), read_content.end());
        std::map<std::string, int> words;
        std::string write_content;
        std::string word;
        for (auto it = content.begin(); ; it++) {
            if (it != content.end() && std::isalpha(*it)) {
                word += *it;
            } else if (!word.empty()) {
                if (words.count(word))
                    words[word]++;
                else
                    words[word] = 1;
                word.clear();
            }
            if (it == content.end())
                break;
        }

        for (const auto &[key, val] : words)
            write_content += key + " " + std::to_string(val) + " ";

        auto mknode_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "m-" + std::to_string(index));
        if (mknode_res.is_err())
            return;
        inode_id = mknode_res.unwrap();

        auto write_res = chfs_client->write_file(inode_id, 0, {write_content.begin(), write_content.end()});
        if (write_res.is_err())
            return;

        doSubmit(MAP, index);
    }

    void Worker::doReduce(int index, int nfiles, int nreduces) {
        std::map<std::string, int> words;
        std::string content;

        for (int i = 0; i < nfiles; i++) {
            auto loopup_res = chfs_client->lookup(1, "m-" + std::to_string(i));
            if (loopup_res.is_err())
                continue;
            auto inode_id = loopup_res.unwrap();

            auto type_attr_res = chfs_client->get_type_attr(inode_id);
            if (type_attr_res.is_err())
                continue;
            auto [type, attr] = type_attr_res.unwrap();

            auto read_res = chfs_client->read_file(inode_id, 0, attr.size);
            if (read_res.is_err())
                continue;
            auto content = read_res.unwrap();

            std::stringstream ss({content.begin(), content.end()});
            std::string key;
            int val;
            const int begin = (26 / nreduces) * index;
            const int end = index == nreduces - 1 ? 26 : begin + (26 / nreduces);
            while (ss >> key >> val)
                if ((key[0] >= 'a' + begin && key[0] < 'a' + end) || (key[0] >= 'A' + begin && key[0] < 'A' + end)) {
                    if (words.count(key))
                        words[key] += val;
                    else
                        words[key] = val;
                }
        }

        for (const auto &[key, val] : words)
            content += key + " " + std::to_string(val) + " ";

        auto mknode_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, "r-" + std::to_string(index));
        if (mknode_res.is_err())
            return;
        auto inode_id = mknode_res.unwrap();

        auto write_res = chfs_client->write_file(inode_id, 0, {content.begin(), content.end()});
        if (write_res.is_err())
            return;
        
        doSubmit(REDUCE, index);
    }

    void Worker::doSubmit(mr_tasktype taskType, int index) {
        mr_client->call(SUBMIT_TASK, static_cast<int>(taskType), index);
    }

    void Worker::stop() {
        shouldStop = true;
        work_thread->join();
    }

    void Worker::doWork() {
        while (!shouldStop) {
            auto asktask_res = mr_client->call(ASK_TASK, 0);
            if (asktask_res.is_err()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            auto [type, index, filename, nfiles, nreduces] = asktask_res.unwrap()->as<std::tuple<int, int, std::string, int, int>>();

            if (index == -1)
                return;

            if (type == NONE)
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            else if (type == MAP)
                doMap(index, filename);
            else if (type == REDUCE)
                doReduce(index, nfiles, nreduces);
        }
    }
}