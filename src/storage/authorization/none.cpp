#include "storage/authorization/none.hpp"
#include "api_utils.hpp"
#include "storage/irc_catalog.hpp"

namespace duckdb {

NoneAuthorization::NoneAuthorization() : IRCAuthorization(IRCAuthorizationType::NONE) {
}

unique_ptr<IRCAuthorization> NoneAuthorization::FromAttachOptions(IcebergAttachOptions &input) {
	auto result = make_uniq<NoneAuthorization>();
	return result;
}

unique_ptr<HTTPResponse> NoneAuthorization::GetRequest(ClientContext &context,
                                                       const IRCEndpointBuilder &endpoint_builder) {
	return APIUtils::GetRequest(context, endpoint_builder, "");
}

} // namespace duckdb
