#pragma once

#include "common/thread_pool.h"
#include "network/network_manager.h"

namespace peloton {

    //===--------------------------------------------------------------------===//
// Global Setup and Teardown
//===--------------------------------------------------------------------===//

    class PelotonMain {
    private:
        peloton::ThreadPool thread_pool;
        peloton::network::NetworkManager network_manager;
    public:
        void Initialize();

        void Shutdown();

        static void SetUpThread();

        static void TearDownThread();

        static PelotonMain &GetInstance();

        ThreadPool &GetThreadPool();

//        network::NetworkManager &GetNetworkManager();
    };

}