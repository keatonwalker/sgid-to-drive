import os
import json
import argparse


PACKAGE_JSON_FOLDER = 'packages'
FEATURE_SPEC_FOLDER = 'features'
FEATURE_SPEC_TEMPLATE = 'templates/feature_template.json'
PACKAGE_SPEC_TEMPLATE = 'templates/package_template.json'


def valitdate_spec(spec):
    if "" in spec['parent_ids']:
        raise Exception('Invalid spec: {}'.format(spec))


def save_spec_json(json_path, spec):
    with open(json_path, 'w') as f_out:
        # spec['upload_date'] = strftime("%Y_%m_%d")
        f_out.write(json.dumps(spec, sort_keys=True, indent=4))


def load_feature_json(json_path):
    with open(json_path, 'r') as json_file:
        feature = json.load(json_file)

    return feature


def create_feature_spec_name(source_name):
    spec_name = '_'.join(source_name.split('.')[-2:]) + '.json'
    return spec_name


def create_package_spec(name, feature_classes, category):
    if not os.path.exists(os.path.join('packages', name + '.json')):
        empty_spec = PACKAGE_SPEC_TEMPLATE
        package = load_feature_json(empty_spec)
        package['name'] = name
        package['feature_classes'] = feature_classes
        package['category'] = category

        save_spec_json(os.path.join('packages', package['name'] + '.json'),
                       package)


def get_package(package_name):
    spec_name = package_name
    if package_name.find('.json') == -1:
        spec_name += '.json'

    package_spec = os.path.join('packages', spec_name)

    package = None
    if not os.path.exists(package_spec):
        raise Exception('Package spec does not exist at {}'.format(package_spec))
    else:
        package = load_feature_json(package_spec)
    valitdate_spec(package)

    return package


def get_feature(source_name, packages=[]):
    empty_spec = FEATURE_SPEC_TEMPLATE
    spec_name = create_feature_spec_name(source_name)
    feature_spec = os.path.join(FEATURE_SPEC_FOLDER, spec_name)
    feature = None
    if not os.path.exists(feature_spec):
        feature = load_feature_json(empty_spec)
        feature['sgid_name'] = source_name
        feature['name'] = source_name.split('.')[-1]
        feature['category'] = source_name.split('.')[-2]
        feature['packages'].extend(packages)
    else:
        feature = load_feature_json(feature_spec)
        for p in packages:
            if p not in feature['packages']:
                feature['packages'].append(p)

    save_spec_json(feature_spec, feature)
    return feature


def add_feature_to_package(package_name, feature_source_name):
    package = get_package(package_name)
    if feature_source_name not in package['feature_classes']:
        package['feature_classes'].append(feature_source_name)
    save_spec_json(os.path.join('packages', package['name'] + '.json'),
                   package)


def add_package_to_feature(source_name, package_name):
    add_feature_to_package(package_name, source_name)
    feature = get_feature(source_name, [package_name])
    save_spec_json(os.path.join(FEATURE_SPEC_FOLDER,
                                create_feature_spec_name(source_name)),
                   feature)


def create_new_jsons(old_json_path):
    for root, subdirs, files in os.walk(old_json_path):
        for filename in files:
            try:
                feature_spec = load_feature_json(os.path.join(root, filename))
            except ValueError, e:
                print filename
                continue

            if len(feature_spec) > 1:
                print filename

            feature_spec = feature_spec[0]
            feature_list = feature_spec['feature_classes']
            if "" in feature_list:
                feature_list.remove("")
            if len(feature_list) > 1:
                for fc in feature_list:
                    create_package_spec(feature_spec['Name'], feature_list, feature_spec['Category'])
            else:
                get_feature(feature_list[0])
        break


def get_package_spec_list():
    packages = []
    for root, subdirs, files in os.walk(PACKAGE_JSON_FOLDER):
        for filename in files:
            if filename == '.DS_Store':
                continue
            packages.append(filename)
        break
    return packages


def list_packages_with_nonexistant_features(workspace):
    import arcpy
    bad_features = []
    bad_packages = {}
    packages_to_check = get_package_spec_list()

    for p in packages_to_check:
        if not p.endswith('.json'):
            p += '.json'
        packages_spec = get_package(p)
        fcs = packages_spec['feature_classes']
        if fcs != '' and len(fcs) > 0:
            for f in fcs:
                if f not in bad_features and not arcpy.Exists(os.path.join(workspace, f)):
                    print f
                    bad_features.append(f)
                    if p not in bad_packages:
                        bad_packages[p] = []
                    bad_packages[p].append(f)
    for p in bad_packages:
        print p
        for f in bad_packages[p]:
            print ' ', f


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Update metadata from docs')#, parents=[tools.argparser])

    parser.add_argument('--add_package', action='store', dest='package_name',
                        help='Add a package to feature spec')
    parser.add_argument('source_name', action='store',
                        help='Source name for the feature')
    parser.add_argument('-c', action='store_true', dest='create',
                        help='Create a feature spec if it does not exist')

    args = parser.parse_args()
    if args.create:
        get_feature(args.source_name)

    if args.package_name:
        add_package_to_feature(args.source_name, args.package_name)


    # list_packages_with_nonexistant_features("Database Connections\Connection to sgid.agrc.utah.gov.sde")
