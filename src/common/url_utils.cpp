#include "url_utils.hpp"
#include "../include/url_utils.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

void IRCEndpointBuilder::AddPathComponent(const string &component) {
	if (!component.empty()) {
		path_components.push_back(component);
	}
}

string IRCEndpointBuilder::GetHost() const {
	return host;
}

void IRCEndpointBuilder::SetHost(const string &host_) {
	host = host_;
}

void IRCEndpointBuilder::SetParam(const string &key, const string &value) {
	params[key] = value;
}

string IRCEndpointBuilder::GetParam(const string &key) const {
	if (params.find(key) != params.end()) {
		return params.at(key);
	}
	return "";
}

const std::unordered_map<string, string> IRCEndpointBuilder::GetParams() const {
	return params;
}

string IRCEndpointBuilder::GetURL() const {
	//! {host}[/{version}][/{prefix}]/{path_component[0]}/{path_component[1]}
	string ret = host;
	for (auto &component : path_components) {
		ret += "/" + component;
	}

	// encode params
	auto sep = "?";
	if (params.size() > 0) {
		for (auto &param : params) {
			auto key = StringUtil::URLEncode(param.first);
			auto value = StringUtil::URLEncode(param.second);
			ret += sep + key + "=" + value;
			sep = "&";
		}
	}
	return ret;
}

IRCEndpointBuilder IRCEndpointBuilder::FromURL(const string &url) {
	auto ret = IRCEndpointBuilder();
	size_t schemeEnd = url.find("://");
	if (schemeEnd == string::npos) {
		throw InvalidInputException("Invalid URL: missing scheme");
	}
	string scheme = url.substr(0, schemeEnd);

	// Start of host
	size_t hostStart = schemeEnd + 3;

	// Find where host ends (at first '/' after scheme)
	size_t pathStart = url.find('/', hostStart);
	ret.SetHost(url.substr(0, pathStart));

	// Extract path and split into components
	string path = url.substr(pathStart + 1);
	size_t pos = 0;
	string component = "";
	while ((pos = path.find('/')) != string::npos) {
		component = path.substr(0, pos);
		if (!component.empty()) {
			ret.path_components.push_back(component);
		}
		path.erase(0, pos + 1);
	}
	if (!path.empty()) {
		ret.path_components.push_back(path);
	}
	D_ASSERT(ret.GetURL() == url);
	return ret;
}

} // namespace duckdb
