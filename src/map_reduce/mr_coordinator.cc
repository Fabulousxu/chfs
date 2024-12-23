#include <string>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <mutex>

#include "map_reduce/protocol.h"

namespace mapReduce {
    std::tuple<int, int, std::string, int, int> Coordinator::askTask(int) {
        std::unique_lock<std::mutex> lock(client_mtx);
        
        if (!isMapFinished) {
            if (mapIndex++ < files.size())
                return {MAP, mapIndex - 1, files[mapIndex - 1], 0, 0};
            return {NONE, 0, "", 0, 0};
        }
        
        if (!isReduceFinished) {
            if (reduceIndex++ < nReduces)
                return {REDUCE, reduceIndex - 1, "", files.size(), nReduces};
            return {NONE, 0, "", 0, 0};
        }

        return {NONE, -1, "", 0, 0};
    }

    int Coordinator::submitTask(int taskType, int index) {
        std::unique_lock<std::mutex> lock(client_mtx);

        if (taskType == MAP) {
            if (index == files.size() - 1)
                isMapFinished = true;
        } else if (taskType == REDUCE) {
            if (index == nReduces - 1)
                isReduceFinished = true;
        }

        if (isReduceFinished) {
            std::string write_content;

            auto loopup_res = chfs_client->lookup(1, outPutFile);
            chfs::inode_id_t inode_id;
            if (loopup_res.is_err()) {
                auto mknode_res = chfs_client->mknode(chfs::ChfsClient::FileType::REGULAR, 1, outPutFile);
                if (mknode_res.is_err()) {
                    isFinished = true;
                    mtx.unlock();
                    return 0;
                }
                inode_id = mknode_res.unwrap();
            } else
                inode_id = loopup_res.unwrap();

            for (int i = 0; i < nReduces; i++) {
                auto look_res = chfs_client->lookup(1, "r-" + std::to_string(i));
                if (look_res.is_err())
                    continue;
                auto inode_id = look_res.unwrap();

                auto type_attr_res = chfs_client->get_type_attr(inode_id);
                if (type_attr_res.is_err())
                    continue;
                auto [type, attr] = type_attr_res.unwrap();

                auto read_res = chfs_client->read_file(inode_id, 0, attr.size);
                if (read_res.is_err())
                    continue;
                auto content = read_res.unwrap();

                write_content.insert(write_content.end(), content.begin(), content.end());
            }

            chfs_client->write_file(inode_id, 0, {write_content.begin(), write_content.end()});
            isFinished = true;
            mtx.unlock();
        }

        return 0;
    }

    // mr_coordinator calls Done() periodically to find out
    // if the entire job has finished.
    bool Coordinator::Done() {
        std::unique_lock<std::mutex> uniqueLock(this->mtx);
        return this->isFinished;
    }

    // create a Coordinator.
    // nReduce is the number of reduce tasks to use.
    Coordinator::Coordinator(MR_CoordinatorConfig config, const std::vector<std::string> &files, int nReduce) {
        this->files = files;
        this->isFinished = false;
        isMapFinished = false;
        isReduceFinished = false;
        nReduces = nReduce;
        mapIndex = 0;
        reduceIndex = 0;
        outPutFile = config.resultFile;
        chfs_client = config.client;
        mtx.lock();
        rpc_server = std::make_unique<chfs::RpcServer>(config.ip_address, config.port);
        rpc_server->bind(ASK_TASK, [this](int i) { return this->askTask(i); });
        rpc_server->bind(SUBMIT_TASK, [this](int taskType, int index) { return this->submitTask(taskType, index); });
        rpc_server->run(true, 1);
    }
}