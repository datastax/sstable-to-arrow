#include "timer.h"

instrumentor::instrumentor()
    : m_current_session(nullptr), m_profile_count(0)
{
}

void instrumentor::begin_session(const std::string &name, const std::string &filepath)
{
    m_ostream.open(filepath);
    write_header();
    m_current_session = new instrumentation_session{name};
}

void instrumentor::end_session()
{
    write_footer();
    m_ostream.close();
    delete m_current_session;
    m_current_session = nullptr;
    m_profile_count = 0;
}

void instrumentor::write_profile(const profile_result &result)
{
    std::string name = result.name;
    std::replace(name.begin(), name.end(), '"', '\'');

    std::lock_guard<std::mutex> lockGuard(mutex);

    if (m_profile_count++ > 0)
        m_ostream << ",";

    m_ostream << "{";
    m_ostream << "\"cat\":\"function\",";
    m_ostream << "\"dur\":" << (result.end - result.start) << ',';
    m_ostream << "\"name\":\"" << name << "\",";
    m_ostream << "\"ph\":\"X\",";
    m_ostream << "\"pid\":0,";
    m_ostream << "\"tid\":" << result.thread_id << ",";
    m_ostream << "\"ts\":" << result.start;
    m_ostream << "}";

    m_ostream.flush();
}

// We shouldn't need to lock to prevent race conditions, since this is only
// called once by the main thread.
void instrumentor::write_header()
{
    m_ostream << "{\"otherData\": {},\"traceEvents\":[";
    m_ostream.flush();
}

// Similar to write_header, no need to lock since it is only called by main thread.
void instrumentor::write_footer()
{
    m_ostream << "]}";
    m_ostream.flush();
}

instrumentor &instrumentor::get()
{
    static instrumentor instance;
    return instance;
}

instrumentation_timer::instrumentation_timer(const std::string &fn_name)
    : m_fn_name(fn_name), m_start_time_point(std::chrono::high_resolution_clock::now()), m_stopped(false)
{
}

instrumentation_timer::~instrumentation_timer()
{
    if (m_stopped)
        return;
    auto end_time_point = std::chrono::high_resolution_clock::now();
    auto start = std::chrono::time_point_cast<std::chrono::microseconds>(m_start_time_point).time_since_epoch().count();
    auto end = std::chrono::time_point_cast<std::chrono::microseconds>(end_time_point).time_since_epoch().count();
    uint32_t thread_id = std::hash<std::thread::id>{}(std::this_thread::get_id());
    instrumentor::get().write_profile({m_fn_name, start, end, thread_id});
    m_stopped = true;
}
