'''
Takes a package dictionary and loads into CKAN via the API.
Checks to see if it already exists by name and preferably a unique field in
the extras too.
Uses ckanclient.
'''
import re
import copy
from traceback import format_exc
from pprint import pformat
import itertools

from ckanclient import CkanApiError

PACKAGE_NAME_MAX_LENGTH = 100 # this should match with ckan/model/package.py
                              # but we avoid requiring ckan in this loader.

ACTIVE = 'active'             # should match ckan.model.ACTIVE
                              
log = __import__("logging").getLogger(__name__)

class LoaderError(Exception):
    pass

class PackageLoader(object):
    def __init__(self, ckanclient):
        '''
        Loader for packages into a CKAN server. Takes package dictionaries
        and loads them using the ckanclient. Can also add packages to a
        specified group.

        It checks to see if a package of the same name is already on the
        CKAN server and if so, updates it with the new info. Create a subclass
        implementing _find_package, which determines how an existing package
        is discovered.

        @param ckanclient - ckanclient object, which contains the
                            connection to CKAN server
        '''
        # Note: we pass in the ckanclient (rather than deriving from it), so
        # that we can choose to pass a test client instead of a real one.
        self.ckanclient = ckanclient
    
    def load_package(self, pkg_dict):
        # see if the package is already there
        existing_pkg_name, existing_pkg = self._find_package(pkg_dict)

        # if creating a new package, check the name is available
        if not existing_pkg_name:
            self._ensure_pkg_name_is_available(pkg_dict)

        # write package
        pkg_dict = self._write_package(pkg_dict, existing_pkg_name, existing_pkg)
        pkg_dict = self.ckanclient.last_message
        return pkg_dict

    def load_packages(self, pkg_dicts):
        '''Loads multiple packages.
        @return results and resulting package names/ids.'''
        num_errors = 0
        num_loaded = 0
        pkg_ids = []
        pkg_names = []
        for pkg_dict in pkg_dicts:
            log.info('Loading %s' % pkg_dict['name'])
            try:
                pkg_dict = self.load_package(pkg_dict)
            except LoaderError:
                log.error('Error loading dict "%s":\n%s' % (pkg_dict['name'], format_exc()))
                num_errors += 1
            else:
                pkg_ids.append(pkg_dict['id'])
                pkg_names.append(pkg_dict['name'])
                num_loaded += 1
        return {'pkg_names':pkg_names,
                'pkg_ids':pkg_ids,
                'num_loaded':num_loaded,
                'num_errors':num_errors}

    def _find_package(self, pkg_dict):
        raise NotImplemented

    def _write_package(self, pkg_dict, existing_pkg_name, existing_pkg=None):
        '''
        Writes a package (pkg_dict). If there is an existing package to
        be changed, then supply existing_pkg_name. If the caller has already
        got the existing package then pass it in, to save getting it twice.
        @return pkg_dict - the package as it was written
        '''
        if existing_pkg_name:
            if not existing_pkg:
                existing_pkg = self._get_package(existing_pkg_name)
            if existing_pkg_name != pkg_dict["name"]:
                pkg_dict = pkg_dict.copy()
                pkg_dict["name"] = existing_pkg_name
            if self._pkg_has_changed(existing_pkg, pkg_dict):
                log.info('...updating existing package')
                try:
                    self.ckanclient.package_entity_put(pkg_dict)
                except CkanApiError:
                    raise LoaderError(
                        'Error (%s) editing package over API: %s' % \
                        (self.ckanclient.last_status,
                         self.ckanclient.last_message))
                pkg_dict = self.ckanclient.last_message
            else:
                log.info('...no change')
        else:
            log.info('...creating package')
            try:
                self.ckanclient.package_register_post(pkg_dict)
            except CkanApiError:
                raise LoaderError(
                    'Error (%s) creating package over API: %s' % \
                    (self.ckanclient.last_status,
                     self.ckanclient.last_message))
            pkg_dict = self.ckanclient.last_message
        return pkg_dict

    def add_pkg_to_group(self, pkg_name, group_name):
        return self.add_pkgs_to_group([pkg_name], group_name)

    def add_pkgs_to_group(self, pkg_names, group_name):
        for pkg_name in pkg_names:
            assert not self.ckanclient.is_id(pkg_name), pkg_name
        assert not self.ckanclient.is_id(group_name), group_name
        try:
            group_dict = self.ckanclient.group_entity_get(group_name)
        except CkanApiError, e:
            if self.ckanclient.last_status == 404:
                raise LoaderError('Group named %r does not exist' % group_name)
            else:
                raise LoaderError('Unexpected status (%s) checking for group name %r: %r') % (self.ckanclient.last_status, group_name, group_dict)
        group_dict['packages'] = (group_dict['packages'] or []) + pkg_names
        try:
            group_dict = self.ckanclient.group_entity_put(group_dict)
        except CkanApiError, e:
            raise LoaderError('Unexpected status %s writing to group \'%s\': %r' % (self.ckanclient.last_status, group_dict, e.args))

    def _get_package(self, pkg_name):
        try:
            pkg = self.ckanclient.package_entity_get(pkg_name)
        except CkanApiError, e:
            if self.ckanclient.last_status == 404:
                pkg = None
            else:
                raise LoaderError('Unexpected status %s checking for package under \'%s\': %r' % (self.ckanclient.last_status, pkg_name, e.args))
        return pkg

    def _find_package_by_fields(self, field_keys, pkg_dict):
        '''Looks for a package that has matching keys to the pkg supplied.
        Requires a unique match or it raises LoaderError.
        @return (pkg_name, pkg) - pkg_name - the name of the matching
                                  package or None if there is none.
                                  pkg - the matching package dict if it
                                  happens to have been requested,
                                  otherwise None
        '''
        if field_keys == ['name']:
            pkg = self._get_package(pkg_dict['name'])
            pkg_name = pkg_dict['name'] if pkg else None
        else:
            search_options = self._get_search_options(field_keys, pkg_dict)
            pkg_name, pkg = self._find_package_by_options(search_options)

        if not pkg_name:
            # Just in case search is not being well indexed, look for the
            # package under its name as well
            try_pkg_name = pkg_dict['name']
            pkg = self._get_package(try_pkg_name)
            while pkg:
                if self._pkg_matches_search_options(pkg, search_options):
                    log.warn('Search failed to find package %r with ref %r, '
                             'but luckily the name is what was expected so loader '
                             'found it anyway.' % (pkg_dict['name'], search_options))
                    pkg_name = try_pkg_name
                    break
                try_pkg_name += '_'
                pkg = self._get_package(try_pkg_name)
            else:
                pkg_name = pkg = None
        return pkg_name, pkg 

    def _get_search_options(self, field_keys, pkg_dict):
        search_options = {}
        has_a_value = False
        for field_key in field_keys:
            field_value = pkg_dict.get(field_key) or (pkg_dict['extras'].get(field_key) if pkg_dict.has_key('extras') else None)
            search_options[field_key] = field_value or u''
            if field_value:
                has_a_value = True
        if not has_a_value:
            raise LoaderError('Package %r has blank values for identifying fields: %r' % (pkg_dict['name'], field_keys))
        return search_options
        
    def _package_search(self, search_options):
        try:
            res = self.ckanclient.package_search(q='', search_options=search_options)
        except CkanApiError, e:
            raise LoaderError('Search request failed (status %s): %r' % (self.ckanclient.last_status, e.args))
        return res

    def _find_package_by_options(self, search_options):
        '''The search_options specify values a package must have and this
        returns the package. If more than one package matching the
        search_options is found, then LoaderError is raised. If none match
        then it returns (None, None). If one match is found then it returns:
        (pkg_name, pkg) where pkg may be None, or returned filled, as a
        convenience.

        '''
        search = self._package_search(search_options)
        # Search doesn't do exact match (e.g. sql search searches *in*
        # a field), so check matches thoroughly.
        # Also check the package is active
        exactly_matching_pkg_names = []
        pkg = None
        for pkg_ref in search['results']:
            pkg = self._get_package(pkg_ref)
            if pkg['state'] == ACTIVE and \
                   self._pkg_matches_search_options(pkg, search_options):
                exactly_matching_pkg_names.append(pkg["name"])
        if len(exactly_matching_pkg_names) > 1:
            raise LoaderError('More than one record matches the search options %r: %r' % (search_options, exactly_matching_pkg_names))
        elif len(exactly_matching_pkg_names) == 1:
            pkg_name = exactly_matching_pkg_names[0]
        else:
            pkg_name = None
        # Only carry through value for pkg if it was the last one and only
        # one fetched
        if not(search['count'] == 1 and pkg and pkg['name'] == pkg_name):
            pkg = None
        return pkg_name, pkg

    def _ensure_pkg_name_is_available(self, pkg_dict):
        '''Checks the CKAN db to see if the name for this package has been
        already taken, and if so, changes the pkg_dict to have another
        name that is free.
        @return nothing - changes the name in the pkg_dict itself
        '''
        preferred_name = pkg_dict['name']
        clashing_pkg = self._get_package(pkg_dict['name'])
        original_clashing_pkg = clashing_pkg
        while clashing_pkg:
            if len(pkg_dict['name']) >= PACKAGE_NAME_MAX_LENGTH:
                new_name = pkg_dict['name'].rstrip('_')[:-1]
                new_name = new_name.ljust(PACKAGE_NAME_MAX_LENGTH, '_')
                pkg_dict['name'] = new_name
            else:
                pkg_dict['name'] += '_'
            clashing_pkg = self._get_package(pkg_dict['name'])

        if pkg_dict['name'] != preferred_name:
            log.warn('Name %r already exists so new package renamed '
                     'to %r.' % (preferred_name, pkg_dict['name']))
                
    def _pkg_has_changed(self, existing_value, value):
        changed = False
        if isinstance(value, dict):
            for key, sub_value in value.items():
                if key in ('groups', 'import_source'):
                    # loader doesn't setup groups
                    # import_source changing alone doesn't require an update
                    continue
                existing_sub_value = existing_value.get(key)
                if self._pkg_has_changed(existing_sub_value, sub_value):
                    changed = True
                    break
        elif isinstance(value, list) and \
               isinstance(existing_value, list):
            if len(existing_value) != len(value):
                changed = True
            else:
                for i, sub_value in enumerate(value):
                    if self._pkg_has_changed(existing_value[i], sub_value):
                        changed = True
                        break
        elif (existing_value or None) != (value or None):
            changed = True
            
        if changed:
            return True
        return False

    def _pkg_matches_search_options(self, pkg_dict, search_options):
        '''Returns True if pkg_dict matches all of the search_options.'''
        matches = True
        for key, value in search_options.items():
            if pkg_dict.get(key):
                if (pkg_dict.get(key) or None) != (value or None):
                    matches = False
                    break
            else:
                if (pkg_dict['extras'].get(key) or None) != (value or None):
                    matches = False
                    break
        return matches
        
class ReplaceByNameLoader(PackageLoader):
    '''Loader finds a package based on its name.
    Load replaces the package with the supplied pkg_dict.'''

    def _find_package(self, pkg_dict):
        find_pkg_by_keys = ['name']
        return self._find_package_by_fields(find_pkg_by_keys, pkg_dict)

class ReplaceByExtraFieldLoader(PackageLoader):
    '''Loader finds a package based on a unique id in an extra field.
    Loader replaces the package with the supplied pkg_dict.'''
    def __init__(self, ckanclient, package_id_extra_key):
        super(ReplaceByExtraFieldLoader, self).__init__(ckanclient)
        assert package_id_extra_key
        self.package_id_extra_key = package_id_extra_key

    def _find_package(self, pkg_dict):
        find_pkg_by_keys = [self.package_id_extra_key]
        return self._find_package_by_fields(find_pkg_by_keys, pkg_dict)

class ResourceSeriesLoader(PackageLoader):
    '''Loader finds package based on a specified field and checks to see
    if most fields (listed in field_keys_to_expect_invariant) match the
    pkg_dict. Loader then inserts the resources in the pkg_dict into
    the package and updates any fields that have changed (e.g. last_updated).
    It checks to see if the particular resource is already in the package
    by a custom resource ID which is contained in the description field,
    as a word containing the given prefix.
    @param synonyms - a list of tuples describing values of a field that
                      should be regarded as equal, for when searching for
                      an existing package.
                      e.g. {'department': [('DfE', 'DCSF'), ('DCLG', 'CLG')]}
                      means resources for the department DfE would be inserted
                      into a package which still had the old deparment name
                      of DCSF (and the same for CLG and GCLG).
    '''
    def __init__(self, ckanclient,
                 field_keys_to_find_pkg_by,
                 resource_id_prefix,
                 field_keys_to_expect_invariant=None,
                 synonyms=None):
        super(ResourceSeriesLoader, self).__init__(ckanclient)
        assert field_keys_to_find_pkg_by and resource_id_prefix
        assert isinstance(field_keys_to_find_pkg_by, (list, tuple))
        self.field_keys_to_find_pkg_by = field_keys_to_find_pkg_by
        self.resource_id_prefix = resource_id_prefix
        self.field_keys_to_expect_invariant = field_keys_to_expect_invariant \
                                              or []
        self.synonyms = synonyms or {}

    def _find_package(self, pkg_dict):
        # take a copy of the keys since the find routine may change them
        find_pkg_by_keys = self.field_keys_to_find_pkg_by[:]
        return self._find_package_by_fields(find_pkg_by_keys, pkg_dict)

    def _get_search_options(self, field_keys, pkg_dict):
        search_options = super(ResourceSeriesLoader, self)._get_search_options(field_keys, pkg_dict)
        # now take account of the synonyms to search for
        search_options_list = [search_options]
        for field_key, field_value in search_options.items():
            if field_key in self.synonyms:
                for synonym_list in self.synonyms[field_key]:
                    if field_value in synonym_list:
                        alt_field_values = list(synonym_list)
                        alt_field_values.remove(field_value)
                        for opts in search_options_list[:]:
                            for alt_field_value in alt_field_values:
                                alt_opts = opts.copy()
                                alt_opts[field_key] = alt_field_value
                                search_options_list.append(alt_opts)
        return search_options_list

    def _package_search(self, search_options_list):
        try:
            result_count = 0
            result_generators = []
            for search_options in search_options_list:
                res = self.ckanclient.package_search(q='', search_options=search_options)
                result_count += res['count']
                result_generators.append(res['results'])
        except CkanApiError, e:
            raise LoaderError('Search request failed (status %s): %r' % (self.ckanclient.last_status, e.args))
        return {'count': result_count,
                'results': itertools.chain(*result_generators)}

    def _pkg_matches_search_options(self, pkg_dict, search_options_list):
        '''Returns True if pkg_dict matches any of the search_options
        listed.'''
        matches = False
        for search_options in search_options_list:
            if super(ResourceSeriesLoader, self)._pkg_matches_search_options(pkg_dict, search_options):
                matches = True
                break
        return matches

    def _write_package(self, pkg_dict, existing_pkg_name, existing_pkg=None):
        '''
        Writes a package (pkg_dict). If there is an existing package to
        be changed, then supply existing_pkg_name. If the caller has already
        got the existing package then pass it in, to save getting it twice.
        '''
        if existing_pkg_name:
            if not existing_pkg:
                existing_pkg = self._get_package(existing_pkg_name)
            pkg_dict = self._merge_resources(existing_pkg, pkg_dict)
        super(ResourceSeriesLoader, self)._write_package(pkg_dict,
                                                        existing_pkg_name,
                                                        existing_pkg)

    def _merge_resources(self, existing_pkg, pkg):
        '''Takes an existing_pkg and merges in resources from the pkg.
        '''
        log.info("Merging resources into %s" % existing_pkg["name"])
        log.debug("Existing resources:\n%s" % pformat(existing_pkg["resources"]))
        log.debug("New resources:\n%s" % pformat(pkg["resources"]))

        # check invariant fields aren't different
        warnings = []
        for key in self.field_keys_to_expect_invariant:
            if key in existing_pkg or key in pkg:
                if (existing_pkg.get(key) or None) != (pkg.get(key) or None):
                    warnings.append('%s: %r -> %r' % (key, existing_pkg.get(key), pkg.get(key)))
            else:
                if (existing_pkg['extras'].get(key) or None) != (pkg['extras'].get(key) or None):
                    warnings.append('%s: %r -> %r' % (key, existing_pkg['extras'].get(key), pkg['extras'].get(key)))
                
        if warnings:
            log.warn('Warning: uploading package \'%s\' and surprised to see '
                     'changes in these values:\n%s' % (existing_pkg['name'], 
                                                       '; '.join(warnings)))

        # copy over all fields but use the existing resources
        merged_dict = pkg.copy()
        merged_dict['resources'] = copy.deepcopy(existing_pkg['resources'])

        # merge resources
        for pkg_res in pkg['resources']:
            # look for resource ID already being there
            pkg_res_id = self._get_resource_id(pkg_res)
            for i, existing_res in enumerate(merged_dict['resources']):
                res_id = self._get_resource_id(existing_res)
                if res_id == pkg_res_id:
                    # edit existing resource
                    merged_dict['resources'][i] = pkg_res
                    break
            else:
                # insert new res
                merged_dict['resources'].append(pkg_res)

        log.debug("Merged resources:\n%s" % pformat(merged_dict["resources"]))

        return merged_dict

    def _get_resource_id(self, res):
        words = re.split('\s', res['description'])
        for word in words:
            if word.startswith(self.resource_id_prefix):
                return word[len(self.resource_id_prefix):]
