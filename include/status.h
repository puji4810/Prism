#ifndef STATUS_H_
#define STATUS_H_

#include <expected>
#include <string>

namespace prism {

enum class ErrorCode {
    Ok = 0,
    NotFound,
    Corruption,
    NotSupported,
    InvalidArgument,
    IOError
};

struct Error {
    ErrorCode code;
    std::string message;
    
    Error(ErrorCode c, std::string msg = "") 
        : code(c), message(std::move(msg)) {}
    
    std::string ToString() const {
        std::string result;
        switch (code) {
            case ErrorCode::NotFound: result = "NotFound: "; break;
            case ErrorCode::Corruption: result = "Corruption: "; break;
            case ErrorCode::NotSupported: result = "NotSupported: "; break;
            case ErrorCode::InvalidArgument: result = "InvalidArgument: "; break;
            case ErrorCode::IOError: result = "IOError: "; break;
            default: return "OK";
        }
        return result + message;
    }
};

// For functions that return a value
template<typename T>
using Result = std::expected<T, Error>;

// For no return functions
using Status = std::expected<void, Error>;

inline Status Ok() { return {}; }

inline auto NotFound(std::string msg = "") {
    return std::unexpected(Error{ErrorCode::NotFound, std::move(msg)});
}

inline auto Corruption(std::string msg = "") {
    return std::unexpected(Error{ErrorCode::Corruption, std::move(msg)});
}

inline auto NotSupported(std::string msg = "") {
    return std::unexpected(Error{ErrorCode::NotSupported, std::move(msg)});
}

inline auto InvalidArgument(std::string msg = "") {
    return std::unexpected(Error{ErrorCode::InvalidArgument, std::move(msg)});
}

inline auto IOError(std::string msg = "") {
    return std::unexpected(Error{ErrorCode::IOError, std::move(msg)});
}

} // namespace prism

#endif
