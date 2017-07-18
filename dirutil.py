import os
import hashlib
import re
import json
import shutil

import spec_manager
import driver
user_drive = driver.AgrcDriver(secrets=driver.OAUTH_CLIENT_SECRET_FILE, use_oauth=True)


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

    def get_catname(self):
        catname = self.category + '_' + self.name
        return catname.lower()

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


def replace_ftp_links(top_dir='data/ftplinktest', rewrite_source=False):
    ftp_link_matcher = re.compile(r'[\"\(](ftp://ftp\.agrc\.utah\.gov/UtahSGID_Vector/UTM12_NAD83)(.+?)[\"\)]')
    data_paths = []
    ided_features = get_spec_catnames(spec_manager.get_feature_spec_list(), True)
    ided_packages = get_spec_catnames(spec_manager.get_package_spec_list(), True)

    not_founds = []

    def get_replace_link(link, spec):
        last_7 = link.path[-7:]
        zip_id = None
        if link.ext == '.zip' and last_7 in ['shp.zip', 'gdb.zip']:
            if 'shp' in last_7:
                zip_id = driver.get_download_link(spec['shape_id'])
            elif 'gdb' in last_7:
                zip_id = driver.get_download_link(spec['gdb_id'])
            return '{}'.format(zip_id)
        elif link.ext is None:
            return '{}'.format(driver.get_webview_link(spec['parent_ids'][0]))

    def check_ftp_links_in_file(path, matcher, feature_specs, package_specs, preview_name):
        data_links = []
        lines = []
        with open(path, 'r') as search_file:
            for line in search_file:
                matches = matcher.findall(line)
                if len(matches) > 0:
                    replace_line = line
                    c = 1
                    for m in matches:
                        link = parse_ftp_link(m[1], top_dir)
                        if link is None:
                            print 'other:'
                            print path
                        elif (not link.packaged and link.get_catname() in feature_specs):
                            replace_link = get_replace_link(link, feature_specs[link.get_catname()])
                            if replace_link is None:
                                print 'other:', link.path
                                continue
                            download = m[0] + m[1]
                            replacer = re.compile(download)
                            replace_line = replacer.sub(replace_link, replace_line)
                            c += 1
                            data_links.append(download)
                        elif (link.packaged and link.get_catname() in package_specs):
                            replace_link = get_replace_link(link, package_specs[link.get_catname()])
                            if replace_link is None:
                                print 'other:', link.path
                                continue
                            download = m[0] + m[1]
                            replacer = re.compile(download)
                            replace_line = replacer.sub(replace_link, replace_line)
                            c += 1
                            data_links.append(download)
                        else:
                            print 'not found:', link.path
                            if link.packaged:
                                not_founds.append(link.name)
                            print path

                    lines.append(replace_line)
                else:
                    lines.append(line)

        with open('data/ftplinktest/replaces_preview/preview' + str(preview_name) + '.html', 'w') as re_file:
            for line in lines:
                re_file.write(line)
        if rewrite_source:
            with open(path, 'w') as re_file:
                for line in lines:
                    re_file.write(line)

        return data_links
    preview_count = 0
    for root, dirs, files in os.walk(top_dir, topdown=True):
        for name in files:
            dir_path = os.path.join(root, name)
            links = check_ftp_links_in_file(dir_path, ftp_link_matcher, ided_features, ided_packages, preview_count)
            if links > 0:
                preview_count += 1
            data_paths.extend(links)

    # for n in not_founds:
    #     print n
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

    uniquer = "{}_{}_{}".format(category, name, packaged)
    if uniquer in FtpLink.unique_links:
        print uniquer, link
        return None

    return FtpLink(category, name, packaged, src_dir, ext, link)


def get_spec_catnames(spec_path_list, only_if_gdbid=False):
    import spec_manager
    specs = [spec_manager.load_feature_json(path) for path in spec_path_list]
    catname_dict = {}
    if only_if_gdbid:
        for s in specs:
            if s['gdb_id'] != '':
                catname_dict["{}_{}".format(s['category'], s['name']).lower()] = s
        # catname_list = ["{}_{}".format(x['category'], x['name']).lower() for x in specs if x['gdb_id'] != '']
    else:
        catname_dict["{}_{}".format(s['category'], s['name']).lower()] = s
        # catname_list = ["{}_{}".format(x['category'], x['name']).lower() for x in specs]
    return catname_dict


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


def get_name_folder_id(name, parent_id):
    """Get drive id for a folder with name of category and in parent_id drive folder."""
    category_id = user_drive.get_file_id_by_name_and_directory(name, parent_id)
    if not category_id:
        print 'Creating drive folder: {}'.format(name)
        category_id = user_drive.create_drive_folder(name, [parent_id])

    return category_id


def reassignparents():
    ided_feature_specs = get_spec_catnames(spec_manager.get_feature_spec_list(), True)
    for spec_name in ided_feature_specs:
        print spec_name
        spec = ided_feature_specs[spec_name]
        old_parent_id = spec['parent_ids'][0]
        new_parent_id = get_name_folder_id(spec['name'], old_parent_id)
        user_drive.change_file_parent(spec['gdb_id'], old_parent_id, new_parent_id)
        user_drive.change_file_parent(spec['shape_id'], old_parent_id, new_parent_id)
        spec['parent_ids'] = [new_parent_id]
        spec_manager.save_spec_json(os.path.join(spec_manager.FEATURE_SPEC_FOLDER, spec_name + '.json'), spec)


if __name__ == '__main__':
    home_dir = os.path.expanduser('~')
    if os.path.exists('data/ftplinktest/replaces_preview'):
        shutil.rmtree('data/ftplinktest/replaces_preview')
    os.makedirs('data/ftplinktest/replaces_preview')
    print 'Temp directory removed'

    data_dir = '/Users/kwalker/Documents/repos/gis.utah.gov/data/geoscience'

    paths = replace_ftp_links(data_dir, rewrite_source=False)
    print 'UPDATED'
    for p in paths:
        print p

    # data_dir = os.path.join(home_dir, 'Documents/repos/gis.utah.gov/data')
    # print home_dir
    # datas = get_all_ftp_links(data_dir)
    # post_dir = os.path.join(home_dir, 'Documents/repos/gis.utah.gov/_posts')
    # posts = get_all_ftp_links(post_dir)
    # ftp_links = []
    #
    # for path in datas:
    #     ftp_links.append(parse_ftp_link(path, 'data'))
    # for p in posts:
    #     ftp_links.append(parse_ftp_link(path, '_posts'))
    # ftp_links = [l for l in ftp_links if l is not None]
    # print 'total links', len(ftp_links)
    #
    # datas.extend(posts)
    # unique_links = set(datas)
    # exts = [p[p.rfind('.'):] for p in unique_links if '.' in p]
    # print 'ext files', len(exts)
    # print set(exts)
    # print 'unique links', len(unique_links)
    # ftp_catnames = {}
    # for fl in ftp_links:
    #     if fl.ext is None or fl.ext == '.zip':
    #         ftp_catnames["{}:{}".format(fl.category, fl.name).lower()] = fl
    #
    # spec_catnames = get_feature_catnames()
    # not_found = [ftp_catnames[n] for n in ftp_catnames if n not in spec_catnames]
    # # not_found = create_new_features(spec_catnames, ftp_catnames, r'Database Connections\Connection to sgid.agrc.utah.gov.sde')
    # package_names = [n.replace('.json', '').lower() for n in spec_manager.get_package_spec_list()]
    # not_package = get_not_found_packages(not_found, package_names)
    # print 'non-feature, not found packages', len(not_package)
    #
    # ftp_packaged_data = [f for f in ftp_links if f.packaged]
    # print 'total packagedData paths', len(ftp_packaged_data)
    # not_package_at_all = get_not_found_packages(ftp_packaged_data, package_names)
    # not_package_is_feature = []
    # not_packge_not_feature_paths = [f.path for f in not_package]
    # for p in not_package_at_all:
    #     if p.path[:p.path.rfind('/') + 1] not in not_packge_not_feature_paths:
    #         not_package_is_feature.append(p.path.strip())
    # print 'Not found packages', len(not_package_at_all)
    #
    # problem_paths = [np.path.strip() for np in not_package]
    # problem_paths.extend(not_package_is_feature)
    # problem_paths.sort()
    # print len(set(problem_paths))
    # with open('data/notfound.txt', 'w') as f_out:
    #     for p in problem_paths:
    #         f_out.write(p + ',\n')
