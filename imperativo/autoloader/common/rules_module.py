def _get_rules_as_list_of_dict():
    return [
        {
            "name": "transaction_id_null",
            "constraint": "transaction_id IS NOT NULL AND transaction_id != ''",
            "tag": "validity"
        },
        {
            "name": "client_id_null",
            "constraint": "client_id IS NOT NULL AND client_id != ''",
            "tag": "validity"
        }
    ]


def get_rules(tags: list[str] | str) -> dict[str, str]:

    if not isinstance(tags, (str, list)):
        raise TypeError(f"'tags' deve ser str ou list, recebido: {type(tags).__name__}")

    if isinstance(tags, str):
        tags = [t.strip() for t in tags.split(",")]
    tag_set = set(tags)
    return {
        row['name']: row['constraint']
        for row in _get_rules_as_list_of_dict()
        if row['tag'] in tag_set
    }
