import copy
import json

import requests
from six.moves import urllib

import alooma

MAPPING_MODES = ['AUTO_MAP', 'STRICT', 'FLEXIBLE']


class _Mapper(object):
    def __init__(self, api):
        self.__api = api
        self.__send_request = api._Alooma__send_request

    def get_mapping_mode(self):
        """
        Returns the default mapping mode currently set in the system.
        The mapping mode should be one of the values in
        alooma._mapper.MAPPING_MODES
        """
        url = self.__api._rest_url + 'mapping-mode'
        res = self.__send_request(requests.get, url)
        return res.content.replace('"', '')

    def set_mapping_mode(self, flexible):
        """
        Sets the default mapping mode in the system. The mapping
        mode should be one of the values in alooma._mapper.MAPPING_MODES
        """
        url = self.__api._rest_url + 'mapping-mode'
        res = requests.post(url, json='FLEXIBLE' if flexible else 'STRICT',
                            **self.__api.requests_params)
        return res

    def get_mapping(self, event_type):
        event_type = self.__api.get_event_type(event_type)
        mapping = self.__api.remove_stats(event_type)
        return mapping

    def auto_map(self, event_type, table_name=None, prefix=None,
                 create_table_if_missing=False):
        """
        Automaps an event type to a table in your Redshift. If a table name
        is not supplied, defaults to using the event_type as a table name.
        :param event_type: The event type to map
        :param table_name: The table in Redshift to map the event type to
        :param prefix: Adds a prefix to the table name (i.e. for event type
        "ex" and prefix "Pre", will create the table "pre_ex".
        :param create_table_if_missing: If True, will create the table if it
        doesn't exist
        """
        table_name = table_name if table_name else event_type.lower()
        if prefix:
            table_name = '%s_%s' % (prefix.lower(), table_name)
        quoted_type = urllib.parse.quote(event_type)
        url = '%s/event-types/%s/auto-map' % (self.__api._rest_url, quoted_type)
        auto_map = json.loads(self.__send_request(requests.post, url).content)
        auto_map['mapping']['tableName'] = \
            table_name if table_name else event_type
        auto_map['state'] = 'MAPPED'
        res = self.set_mapping(auto_map, event_type,
                               create_table_if_missing=create_table_if_missing)
        return res

    def map_field(self, schema, field_path, column_name, field_type, non_null,
                  **type_attributes):
        """
        :param  schema: this is the mapping json
        :param  field_path: this would use us to find the keys
        :param  field_type: the field type (VARCHAR, INT, FLOAT...)
        :param  type_attributes:    some field type need different attributes,
                                    for example:
                                        1. INT doesn't need any attributes.
                                        2. VARCHAR need the max column length
        :param column_name: self descriptive
        :param non_null: self descriptive
        :return: new mapping dict with new argument
        """

        field = self.__api.find_field_name(schema, field_path, True)
        self.__api.set_mapping_for_field(field, column_name, field_type,
                                         non_null, **type_attributes)

    def delete_all_event_types(self):
        res = self.get_event_types()
        for event_type in res:
            self.delete_event_type(event_type["name"])

    def delete_event_type(self, event_type):
        event_type = urllib.parse.quote(event_type, safe='')
        url = self.__api._rest_url + 'event-types/{event_type}' \
            .format(event_type=event_type)

        self.__send_request(requests.delete, url)

    def get_event_types(self):
        """
        Returns a dict representation of all the event-types which
        exist in the system
        """
        url = self.__api._rest_url + 'event-types'
        res = self.__send_request(requests.get, url)
        return alooma.parse_response_to_json(res)

    def get_event_type(self, event_type):
        """
        Returns a dict representation of the requested event-type's
        mapping and metadata if it exists
        :param event_type:  The name of the event type
        :return: A dict representation of the event-type's data
        """
        event_type = urllib.parse.quote(event_type, safe='')
        url = self.__api._rest_url + 'event-types/' + event_type

        res = self.__send_request(requests.get, url)
        return alooma.parse_response_to_json(res)

    def discard_event_type(self, event_type):
        """
        Discards an event type in the Mapper, stopping it from being loaded
        to Redshift or being stored in the Restream Queue
        :param event_type: A str representing an existing event type
        """
        event_type_json = {
            "name": event_type,
            "mapping": {
                "isDiscarded": True,
                "tableName": ""
            },
            "fields": [], "mappingMode": "STRICT"
        }
        self.set_mapping(event_type_json, event_type)

    def discard_field(self, mapping, field_path):
        """
        :param mapping: this is the mapping json
        :param field_path:  this would use us to find the keys

        for example:
                        1.  mapping == {"a":1, b:{c:3}}
                        2.  the "c" field_path == a.b.c
        :return: new mapping JSON that the last argument would be discarded
        for example:
                        1.  mapping == {"a":1, b:{c:3}}
                        2.  field_path == "a.b.c"
                        3.  then the mapping would be as the old but the "c"
                            field that would be discarded
        """

        field = self.find_field_name(mapping, field_path)
        if field:
            field["mapping"]["isDiscarded"] = True
            field["mapping"]["columnName"] = ""
            field["mapping"]["columnType"] = None

    def unmap_field(self, mapping, field_path):
        """
        :param mapping: this is the mapping json
        :param field_path:  this would use us to find the keys

        for example:
                        1.  mapping == {"a":1, b:{c:3}}
                        2.  the "c" field_path == a.b.c
        :return: new mapping JSON that the last argument would be removed
        for example:
                        1.  mapping == {"a":1, b:{c:3}}
                        2.  field_path == "a.b.c"
                        3.  then the mapping would be as the old but the "c"
                            field that would be removed -> {"a":1, b:{}}
        """
        field = self.find_field_name(mapping, field_path)
        if field:
            mapping["fields"].remove(field)

    def find_field_name(self, schema, field_path, add_if_missing=False):
        """
        :param schema:  this is the dict that this method run over ot
                        recursively
        :param field_path: this would use us to find the keys
        :param add_if_missing: add the field if missing
        :return:    the field that we wanna find and to do on it some changes.
                    if the field is not found then raise exception
        """

        fields_list = field_path.split('.', 1)
        if not fields_list:
            return None

        current_field = fields_list[0]
        remaining_path = fields_list[1:]

        field = next((field for field in schema["fields"]
                      if field['fieldName'] == current_field), None)
        if field:
            if not remaining_path:
                return field
            return self.__api.find_field_name(field, remaining_path[0])
        elif add_if_missing:
            parent_field = schema
            for field in fields_list:
                parent_field = self.__api.add_field(parent_field, field)
            return parent_field
        else:
            # raise this if the field is not found,
            # not standing with the case ->
            # field["fieldName"] == field_to_find
            raise Exception("Could not find field path")

    def set_mapping(self, mapping, event_type, create_table_if_missing=False):
        """
        Maps an event type to a table in Redshift.
        :param mapping: The mapping to submit
        :param event_type: The name of the event type to map
        :param create_table_if_missing: If True, the table will be created if
        it doesn't exist already
        Mapping example:
          {u'fieldName': u'type',
           u'fields': [],
           u'mapping': {
            u'columnName': u'type',
            u'columnType': {
             u'length': 256,
             u'nonNull': False,
             u'truncate': False,
             u'type': u'VARCHAR'},
            u'isDiscarded': False,
            u'machineGenerated': False}},
          {u'fieldName': u'id',
           u'fields': [],
           u'mapping': {
            u'columnName': u'id',
            u'columnType': {u'nonNull': False, u'type': u'FLOAT'},
            u'isDiscarded': False,
            u'machineGenerated': False}},
          {u'fieldName': u'timestamp',
           u'fields': [],
           u'mapping': {
            u'columnName': u'timestamp',
            u'columnType': {u'nonNull': False, u'type': u'TIMESTAMP'},
            u'isDiscarded': False,
            u'machineGenerated': False}}],
         u'mapping': {
          u'isDiscarded': False,
          u'readOnly': False,
          u'tableName': u'a_table_name'},
         u'mappingMode': u'STRICT',
         u'name': u'event_type_name',
         u'state': u'MAPPED',
         u'usingDefaultMappingMode': True}
        """
        table_name = mapping['mapping']['tableName']
        if create_table_if_missing:
            table = [t['tableName'] for t in self.__api.redshift.get_tables()
                     if t['tableName'] == table_name]
            if not table:
                self.__api.redshift.create_table(table_name, _table_structure_from_mapping(mapping))

        event_type = urllib.parse.quote(event_type, safe='')
        url = self.__api._rest_url + 'event-types/{event_type}/mapping'.format(
                event_type=event_type)
        res = self.__send_request(requests.post, url, json=mapping)
        return res

    @staticmethod
    def set_mapping_for_field(field, column_name,
                              field_type, non_null, **type_attributes):
        column_type = {"type": field_type, "nonNull": non_null}
        column_type.update(type_attributes)
        field["mapping"] = {
            "columnName": column_name,
            "columnType": column_type,
            "isDiscarded": False
        }

    @staticmethod
    def add_field(parent_field, field_name):
        field = {
            "fieldName": field_name,
            "fields": [],
            "mapping": None
        }
        parent_field["fields"].append(field)
        return field


def _table_structure_from_mapping(mapping, primary_keys=None,
                                  sort_keys=None, dist_key=None):
    """
    Receives a mapping and extracts a table structure from it. This table
    structure can then be used to create a new table using the create_table
    method
    :param primary_keys: A list of column names. If they exist in the
    mapping, they will be marked as primary keys in the resulting structure.
    :param sort_keys: A list of column names. If they exist in the
    mapping, they will be marked as sort keys in the resulting structure
    according to the order in the supplied list.
    :param dist_key: A column name. If it exists in the mapping, it will be
    marked as the distribution key in the resulting structure.
    :param mapping: A valid mapping dict
    :return: A valid table structure dict
    """
    def extract_column(mapped_field):
        cols = []
        sort_key_index = 0
        for subfield in mapped_field['fields']:
            cols.extend(extract_column(subfield))
        if 'mapping' in mapped_field and mapped_field['mapping'] and \
                not mapped_field['mapping']['isDiscarded']:
            field_mapping = copy.deepcopy(mapped_field['mapping'])
            [field_mapping.pop(k) for k in field_mapping.keys()
             if k not in ['columnName', 'columnType']]
            col_name = field_mapping['columnName']
            if primary_keys and col_name in primary_keys:
                field_mapping['primaryKey'] = True
            if sort_keys and col_name in sort_keys:
                field_mapping['sortKeyIndex'] = sort_key_index
                sort_key_index += 1
            if dist_key and col_name == dist_key:
                field_mapping['distKey'] = True
            field_mapping['columnType'].pop('truncate', None)
            cols.append(field_mapping)
        return cols

    return extract_column({'fields': mapping['fields']})