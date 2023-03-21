
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <utility>
#include <queue>
#include <thread>


namespace facebook {
namespace cachelib {
//template <typename C>
//struct ReaperAPIWrapper {
//  static bool promote(C& cache, typename C::Item& item) {
//    return cache.promote(item);
//  }
//  template <typename Fn>
//  static void traverseAndExpireItems(C& cache, Fn&& f) {
//    cache.traverseAndExpireItems(std::forward<Fn>(f));
//  }
//};

template <typename CacheT>
struct ThreadPool {
    struct Task {
        virtual void Run() const = 0;
        virtual ~Task() {};
    };   

    template <typename task_, typename... args_ >
    struct RealTask : public Task {
        RealTask( task_&& task, args_&&... args ) : fun_( std::bind( std::forward<task_>(task), std::forward<args_>(args)... ) ) {}
        void Run() const override {
            fun_();
        }
    private:
        decltype( std::bind(std::declval<task_>(), std::declval<args_>()... ) ) fun_;
    };
    
    template < typename task_, typename... args_ >
    void AddTask( task_&& task, args_&&... args ) {
        auto lock = std::unique_lock<std::mutex>{mtx_};
        using FinalTask = RealTask<task_, args_... >;
        q_.push( std::unique_ptr<Task>( new FinalTask( std::forward<task_>(task), std::forward<args_>(args)... ) ) );
    }
    
    ThreadPool() {
        for( auto & t : pool_ )
            t = std::thread( [=] {
                while ( true ) {
                    std::unique_ptr<Task> task;
                    {
                        auto lock = std::unique_lock<std::mutex>{mtx_};
                        if ( q_.empty() && stop_ ) 
                            break;
                        if ( q_.empty() )
                            continue;
                        task = std::move(q_.front());
                        q_.pop();
                    }
                    if (task)
                        task->Run();
                }
            } );
    }
    ~ThreadPool() {
        stop_ = true;
        for( auto & t : pool_ )
            t.join();
    }
private:
    std::queue<std::unique_ptr<Task>> q_;
    std::thread pool_[8]; 
    std::mutex mtx_;
    volatile bool stop_ {};
};

}
}
