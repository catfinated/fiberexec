#include <functional>

namespace fiberexec {
// TODO: Custom type-erased SBO task type
using task = std::function<void()>;
} // namespace fiberexec
