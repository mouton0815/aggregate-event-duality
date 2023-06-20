# Axum
This branch replaces Warp by Axum.

Axum has the following pros compared to Warp:
* Signature for routes is much simpler because Axum passes all request parameters implicitly whereas Warp passes them explicitly as part of the route definition.
* Return signature of route handlers is both clearer and stricter.

On the downside there is:
* Insufficient examples (especially about header handling).
* Handling of (custom) headers is cumbersome. Either define own type or use HeaderMap. Both requires more code than in Warp.
* Passing constants to request handlers is cumbersome. Axum accepts only one ``State`` parameter per request, which must bundle state and constant values.
* It is not possible to create two handlers with same signature and behavior controlled by constant.
