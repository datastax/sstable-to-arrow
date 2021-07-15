#ifndef TIMER_H_
#define TIMER_H_

#include <chrono>
#include <string>
#include <iostream>
#include <fstream>
#include <algorithm>
#include <thread>
#include <mutex>

#if DEBUG
#   define DEBUG_ONLY(msg) msg
#else
#   define DEBUG_ONLY(msg)
#endif

struct profile_result
{
    std::string name;
    long long start, end;
    uint32_t thread_id;
};

struct instrumentation_session
{
    std::string name;
};

// singleton
class instrumentor
{
private:
    instrumentation_session *m_current_session;
    std::ofstream m_ostream;
    int m_profile_count;
    std::mutex mutex;

public:
    instrumentor();

    void begin_session(const std::string &name, const std::string &filepath = "results.json");
    void end_session();
    void write_profile(const profile_result &result);
    void write_header();
    void write_footer();
    static instrumentor &get();
};

class instrumentation_timer
{
private:
    std::string m_fn_name;
    bool m_stopped;
    std::chrono::time_point<std::chrono::high_resolution_clock> m_start_time_point;

public:
    instrumentation_timer(const std::string &fn_name);
    ~instrumentation_timer();
};

#endif
