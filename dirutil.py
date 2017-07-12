import os
import hashlib
import re
import json

import spec_manager

class FtpLink(object):
    unique_links = {}
    def __init__(self, category, name, packaged, src_dir, ext, path):
        self.category = category
        self.name = name
        self.packaged = packaged
        self.src_dir = src_dir
        self.ext = ext
        self.path = path

        FtpLink.unique_links["{}:{}".format(self.category, self.name)] = 0

    def __str__(self):
        return "cat: {}\nname: {}\npack: {}\nsrc: {}\next: {}".format(self.category,
                                                                      self.name,
                                                                      self.packaged,
                                                                      self.src_dir,
                                                                      self.ext)


def get_directory_count(top_dir):
    # top_dir = r'/Volumes/ftp/UtahSGID_Vector'
    dircount = 0
    for root, dirs, files in os.walk(top_dir, topdown=True):
        for name in dirs:
            dir_path = os.path.join(root, name)
            print dir_path
            dircount += 1
    print dircount


def get_file_count(top_dir, ext, size_limit=None):
    file_count = 0
    for root, dirs, files in os.walk(top_dir, topdown=True):
        for name in files:
            dir_path = os.path.join(root, name)
            if name.endswith(ext):
                if size_limit:
                    if os.path.getsize(dir_path) <= size_limit:
                        file_count += 1
                else:
                    file_count += 1

    print 'count: {}, type: {}, size <= {}'.format(file_count, ext, size_limit)


def hash_files(file_list):
    hex_digest = []
    for f in file_list:
            file_path = f
            local_file_hash = hashlib.md5(open(file_path, 'rb').read()).hexdigest()
            hex_digest.append(local_file_hash)

    for h in hex_digest:
        print h

def get_all_ftp_links(top_dir):
    ftp_link_matcher = re.compile(r'[\"\(](ftp://ftp\.agrc\.utah\.gov/UtahSGID_Vector/UTM12_NAD83)(.+?)[\"\)]')
    data_paths = []

    def get_ftp_link_in_file(path, matcher):
        data_links = []
        with open(path, 'r') as search_file:
            for line in search_file:
                matches = matcher.findall(line)
                if len(matches) > 0:
                    data_links.extend([m[1] for m in matches])

        return data_links

    for root, dirs, files in os.walk(top_dir, topdown=True):
        for name in files:
            dir_path = os.path.join(root, name)
            links = get_ftp_link_in_file(dir_path, ftp_link_matcher)
            data_paths.extend(links)

    return data_paths


def parse_ftp_link(link, src_dir):
    link_parts = link.split('/')[1:]
    category = link_parts[0].upper()
    name = None
    packaged = None
    src_dir = src_dir
    ext = None
    if link_parts[1].lower() == 'packageddata':
        packaged = True
        name = link_parts[3]
    elif link_parts[1].lower() == 'unpackageddata':
        packaged = False
        name = link_parts[2]
    else:
        return None

    if '.' in link:
        ext = link[link.rfind('.'):]

    uniquer = "{}:{}:{}".format(category, name, packaged)
    if uniquer in FtpLink.unique_links:
        print uniquer, link
        return None

    return FtpLink(category, name, packaged, src_dir, ext, link)


def get_feature_catnames():
    import spec_manager
    specs = [spec_manager.load_feature_json(path) for path in spec_manager.get_feature_spec_list()]
    return ["{}:{}".format(x['category'], x['name']).lower() for x in specs]


def create_new_features(spec_catnames, ftp_catname_dict, workspace):
    import arcpy
    not_found = []
    for catname in ftp_catname_dict:
        if catname not in spec_catnames:
            ftp_link = ftp_catname_dict[catname]
            fc = "{}.{}.{}".format('SGID10', ftp_link.category.upper(), ftp_link.name)
            if arcpy.Exists(os.path.join(workspace, fc)):
                spec_manager.get_feature(fc)
                print catname
            else:
                not_found.append(ftp_link)
    return not_found


def get_not_found_packages(non_feature_ftp_links, package_names):
    not_package = []
    not_found = non_feature_ftp_links
    for nf in not_found:
        if nf.name.lower() not in package_names:
            not_package.append(nf)

    return not_package


if __name__ == '__main__':
    home_dir = os.path.expanduser('~')
    data_dir = os.path.join(home_dir, 'Documents/repos/gis.utah.gov/data')
    print home_dir
    datas = get_all_ftp_links(data_dir)
    post_dir = os.path.join(home_dir, 'Documents/repos/gis.utah.gov/_posts')
    posts = get_all_ftp_links(post_dir)
    ftp_links = []

    for path in datas:
        ftp_links.append(parse_ftp_link(path, 'data'))
    for p in posts:
        ftp_links.append(parse_ftp_link(path, '_posts'))
    ftp_links = [l for l in ftp_links if l is not None]
    print 'total links', len(ftp_links)

    datas.extend(posts)
    unique_links = set(datas)
    # exts = [p[p.rfind('.'):] for p in unique_links if '.' in p]
    # print 'ext files', len(exts)
    # print set(exts)
    print 'unique links', len(unique_links)
    ftp_catnames = {}
    for fl in ftp_links:
        if fl.ext is None or fl.ext == '.zip':
            ftp_catnames["{}:{}".format(fl.category, fl.name).lower()] = fl

    spec_catnames = get_feature_catnames()
    not_found = [ftp_catnames[n] for n in ftp_catnames if n not in spec_catnames]
    # not_found = create_new_features(spec_catnames, ftp_catnames, r'Database Connections\Connection to sgid.agrc.utah.gov.sde')
    package_names = [n.replace('.json', '').lower() for n in spec_manager.get_package_spec_list()]
    not_package = get_not_found_packages(not_found, package_names)
    print 'non-feature, not found packages', len(not_package)

    ftp_packaged_data = [f for f in ftp_links if f.packaged]
    print 'total packagedData paths', len(ftp_packaged_data)
    not_package_at_all = get_not_found_packages(ftp_packaged_data, package_names)
    not_package_is_feature = []
    not_packge_not_feature_paths = [f.path for f in not_package]
    for p in not_package_at_all:
        if p.path[:p.path.rfind('/') + 1] not in not_packge_not_feature_paths:
            not_package_is_feature.append(p.path.strip())
    print 'Not found packages', len(not_package_at_all)

    problem_paths = [np.path.strip() for np in not_package]
    problem_paths.extend(not_package_is_feature)
    problem_paths.sort()
    print len(set(problem_paths))
    with open('data/notfound.txt', 'w') as f_out:
        for p in problem_paths:
            f_out.write(p + ',\n')
