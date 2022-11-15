# from gradesens.moonstone_external_source import HttpRequestProcessor


# def test_http_request_processor():
#     proc = HttpRequestProcessor(
#         url_pattern="https://lorem.ipsum/{dolor}/{sit}amet{consectetur}",
#         query_string_patterns=(
#             ("adipiscing", "elit"),
#             ("sed{do}", "{eiusmod}tempor"),
#         ),
#     )
#     params = {
#         "dolor": "ut",
#         "sit": "labore",
#         "consectetur": "et",
#         "do": "dolore",
#         "eiusmod": "magna",
#     }
#     assert proc.get_url(params) == ("https://lorem.ipsum/ut/laboreametet")
#     assert proc.get_query_string_params(params) == (
#         ("adipiscing", "elit"),
#         ("seddolore", "magnatempor"),
#     )
