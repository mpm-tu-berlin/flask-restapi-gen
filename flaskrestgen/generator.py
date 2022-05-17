__author__ = 'robusgauli@gmail.com'

import collections
import functools
import itertools
import json
import os
import re
import sys
import urllib.parse

from flask import jsonify
from flask import request
from flaskrestgen.envelop import (
    fatal_error_envelop,
    json_records_envelop,
    record_updated_envelop,
    record_created_envelop,
    record_notfound_envelop,
    record_exists_envelop,
    record_deleted_envelop,
    data_error_envelop,
    validation_error_envelop
)
from flaskrestgen.errors import (
    PrimaryKeyNotFound
)
from sqlalchemy.exc import DataError
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.relationships import RelationshipProperty

format_error = lambda _em: \
    re.search(r'\n.*\n', _em).group(0).strip().capitalize()

format_data_error = lambda _em: \
    re.search(r'\).*\n', _em).group(0)[1:].strip().capitalize()

valid_file = lambda v_file: os.path.exists(v_file) \
                            and os.path.isfile(v_file) and os.path.splitext(v_file)[1] == '.json'


class RESTApi:
    def __init__(self, app, Session, validation_file=None, uri_prefix=None):

        self.app = app
        self.Session = Session

        if validation_file is not None and valid_file(validation_file):
            # this is valid json file
            self._validation = json.loads(open(validation_file).read())
        else:
            self._validation = None

        if uri_prefix is not None:
            self.uri_prefix = uri_prefix
        else:
            self.uri_prefix = None

    def get_for(self, model,
                before_response_for_resources=None,
                before_response_for_resource=None, *,
                extract=None,
                relationship=False,
                extractfor_resources=None,
                decorator_for_resources=None,
                decorator_for_resource=None):

        if not model.__mapper__.primary_key:
            raise PrimaryKeyNotFound('Primary key not found in %s table' % model.__tablename__)
        # if len(model.__mapper__.primary_key) > 1:
        #    raise PrimaryKeyNotFound('Composite primary key found in %s table' % model.__tablename__)

        _primary_keys = [x.name for x in model.__mapper__.primary_key]

        if extract:
            extract = list(extract)

        def _get_resources():
            db_session = self.Session()
            try:
                results = db_session.query(model).all()

                if not extractfor_resources:

                    _list_data_exp = ({key: val for key, val in vars(r).items()
                                       if not key.startswith('_sa')
                                       } for r in results)
                    # inject the URI to the data
                    if self.uri_prefix:
                        _list_data = list({**adict, 'uri': '/%s/%s/%s' % (self.uri_prefix,
                                                                          model.__tablename__,
                                                                          urllib.parse.quote_plus('+'.join(
                                                                              [str(adict[pkey]) for pkey in
                                                                               _primary_keys])))}
                                          for adict in _list_data_exp)
                    else:
                        _list_data = list({**adict, 'uri': '/%s/%s' % (model.__tablename__, urllib.parse.quote_plus(
                            '+'.join([str(adict[pkey]) for pkey in _primary_keys])))}
                                          for adict in _list_data_exp)
                    # if after request if not not then call the predicate
                    if before_response_for_resources:
                        before_response_for_resources(_list_data)

                    return json_records_envelop(_list_data)
                else:
                    raise NotImplementedError("Untested code, proceed with care!")
                    _extractfor_resources = list(extractfor_resources)

                    _list_data = []
                    for result in results:
                        _adict = {key: val for key, val in vars(result).items() if not key.startswith('_')}
                        adict = {**_adict, 'uri': '/%s/%s' % (model.__tablename__, urllib.parse.quote_plus(
                            '+'.join([str(_adict[pkey]) for pkey in _adict[_primary_keys]])))}
                        # nod for each extract with the many to one relationship,
                        for relationship in _extractfor_resources:
                            _rel_val = getattr(result, relationship)
                            if not _rel_val:
                                adict[relationship] = None
                                continue
                            if not isinstance(_rel_val, collections.Iterable):
                                adict[relationship] = {key: val for key, val in vars(_rel_val).items()
                                                       if not key.startswith('_')}
                                continue

                            adict[relationship] = list({key: val for key, val in vars(_r_val).items()
                                                        if not key.startswith('_')}
                                                       for _r_val in _rel_val)

                        # finally add to the list
                        _list_data.append(adict)
                    return json_records_envelop(_list_data)
            finally:
                db_session.close()
                self.Session.remove()

        _get_resources.__name__ = 'get_all' + model.__tablename__

        if decorator_for_resources and isinstance(decorator_for_resources,
                                                  collections.Iterable):
            for decorator in decorator_for_resources:
                _get_resources = decorator(_get_resources)
        elif decorator_for_resources:
            _get_resources = decorator_for_resources(_get_resources)

        if self.uri_prefix:
            self.app.route('/%s/%s' % (self.uri_prefix, model.__tablename__))(_get_resources)
        else:
            self.app.route('/%s' % model.__tablename__)(_get_resources)

        def _get_resource(r_id):
            db_session = self.Session()
            try:
                result = db_session.query(model)
                sub_pks = r_id.split("+")
                for i in range(len(model.__mapper__.primary_key)):
                    result = result.filter(model.__mapper__.primary_key[i] == sub_pks[i])
                result = result.one()
                _data = {
                    key: val for key, val in vars(result).items()
                    if not key.startswith('_sa')
                }

                if before_response_for_resource:
                    before_response_for_resource(result, _data)

                if extract:
                    for relationship in extract:
                        # get the attribute
                        children = getattr(result, relationship)
                        if not children:
                            _data[relationship] = None
                            continue
                        if not isinstance(children, collections.Iterable):
                            # that means it is on many to one side
                            _data[relationship] = {key: val for key, val in
                                                   vars(children).items() if not
                                                   key.startswith('_')}
                            continue

                        _data[relationship] = list({key: val for key, val
                                                    in vars(child).items() if not
                                                    key.startswith('_')} for child in children)


            except NoResultFound:
                return record_notfound_envelop()
            else:
                return json_records_envelop(_data)
            finally:
                db_session.close()
                self.Session.remove()

        _get_resource.__name__ = 'get' + model.__tablename__
        if decorator_for_resource and isinstance(decorator_for_resource,
                                                 collections.Iterable):
            for decorator in decorator_for_resource:
                _get_resource = decorator(_get_resource)
        elif decorator_for_resource:
            _get_resource = decorator_for_resource(_get_resource)
        if self.uri_prefix:
            self.app.route('/%s/%s/<r_id>' % (self.uri_prefix, model.__tablename__))(_get_resource)
        else:
            self.app.route('/%s/<r_id>' % model.__tablename__)(_get_resource)

        if relationship:
            # loads the relationship information
            db_session = self.Session()
            try:
                db_session.query(model)
            finally:
                db_session.close()
                self.Session.remove()
            ##get the relatioship atributes with the direction having 'ONE TO MANY'

            _props = list((attr, rel_prop.mapper) for attr, rel_prop in
                          model.__mapper__._props.items() if isinstance(rel_prop, RelationshipProperty)
                          and rel_prop.direction.name == 'ONETOMANY')

            for _prop, rel_prop in _props:
                # create a nested uri for the each _prop

                # We need to create this in here so it down't disappear
                mappers = {_prop: rel_prop for _prop, rel_prop in _props}

                def _get_resources_by_parent(id):
                    request_target = request.full_path.split("/")[-1].replace("?", "")
                    sub_pks = id.split("+")
                    db_session = self.Session()
                    try:
                        children = db_session.query(mappers[request_target]).join(model)
                        for i in range(len(model.__mapper__.primary_key)):
                            children = children.filter(model.__mapper__.primary_key[i] == sub_pks[i])
                        children = children.all()
                    except NoResultFound:
                        return record_notfound_envelop()
                    else:
                        _list = list({key: val for key, val in vars(data).items() if not key.startswith('_')}
                                     for data in children)
                        _primary_keys = [x.name for x in mappers[request_target].class_.__mapper__.primary_key]
                        # inject the URI to the data
                        if self.uri_prefix:
                            _list = list({**adict, 'uri': '/%s/%s/%s' % (self.uri_prefix,
                                                                         str(mappers[request_target].mapped_table.name),
                                                                         urllib.parse.quote_plus('+'.join(
                                                                             [str(adict[pkey]) for pkey in
                                                                              _primary_keys])))}
                                         for adict in _list)
                        else:
                            _list = list({**adict, 'uri': '/%s/%s' % (
                                str(mappers[request_target].mapped_table.name), urllib.parse.quote_plus('+'.join(
                                    [str(adict[pkey]) for pkey in
                                     _primary_keys])))}
                                         for adict in _list)

                        # if after request if not not then call the predicate
                        if before_response_for_resources:
                            before_response_for_resources(_list)
                        return json_records_envelop(_list)
                    finally:
                        db_session.close()
                        self.Session.remove()

                _get_resources_by_parent.__name__ = 'get' + _prop + 'by' + model.__tablename__
                if self.uri_prefix:
                    self.app.route('/%s/%s/<id>/%s' % (self.uri_prefix, model.__tablename__, _prop))(
                        _get_resources_by_parent)
                else:
                    self.app.route('/%s/<id>/%s' % (model.__tablename__, _prop))(_get_resources_by_parent)

