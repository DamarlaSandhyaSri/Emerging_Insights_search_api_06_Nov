class JsonMapperHelper:
    ''''''
    @classmethod
    def _os_data_validation(cls,data):
        if data is None:
            raise ValueError("Invalid data: data cannot be None")
 
        if not isinstance(data, list):
            raise ValueError(f"Invalid data: expected list, got {type(data).__name__}")
 
        if not data:
            raise ValueError("Invalid data: list cannot be empty")
 
        non_dict_items = []
        for i, item in enumerate(data):
            if not isinstance(item, dict):
                non_dict_items.append(f"index {i}: {type(item).__name__}")
 
        if non_dict_items:
            raise ValueError(f"Invalid data: non-dictionary items found at {', '.join(non_dict_items)}")
 
        return data
 
    @classmethod
    def _equality_check(cls,list1, list2):
        if not isinstance(list1, list) or not isinstance(list2, list):
            raise TypeError("Both inputs must be lists")
 
        if len(list1) != len(list2):
            raise ValueError(f"Lists should be equal in length: {len(list1)} != {len(list2)}")
 
        return True
    @classmethod
    def map_data(cls, data,fields_to_extract,mapper_name):
        '''Args:
            data: list of dicts
            fields_to_extract: list of fields to extract from data
            mapper_name: list of new field names corresponding to fields_to_extract'''
 
        data = cls._os_data_validation(data)
        if cls._equality_check(fields_to_extract, mapper_name):
            mapped = []
            for hits in data:
                source = hits.get("_source", {})
                mapped_item = {v: source.get(k) for k, v in zip(fields_to_extract, mapper_name)}
                mapped.append(mapped_item)
        return mapped
 