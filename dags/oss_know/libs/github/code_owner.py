import json
import re
from concurrent.futures import ThreadPoolExecutor
from os import path, makedirs

import docutils.frontend
import docutils.nodes
import docutils.parsers.rst
import docutils.utils
import git

from oss_know.libs.util.clickhouse_driver import CKServer
from oss_know.libs.util.log import logger


class CodeOwnerWatcher:
    TARGET_FILES = {}
    BATCH_SIZE = 5000
    OWNER = ''
    REPO = ''
    ORIGIN = ''

    def __init__(self, local_repo_dir, ck_conn_info, rev_list_params=None):
        self.local_repo_path = f'{local_repo_dir}/{self.__class__.OWNER}/{self.__class__.REPO}'
        logger.info(f'Init repo {self.__class__.OWNER}/{self.__class__.REPO} to {self.local_repo_path}')
        self.git_repo = None
        self.rev_list_params = rev_list_params if rev_list_params else []
        self.envolved_commits_map = {}
        self.envolved_commits = []
        self.ck_client = CKServer(
            host=ck_conn_info['HOST'],
            port=ck_conn_info['PORT'],
            user=ck_conn_info['USER'],
            password=ck_conn_info['PASSWD'],
            database=ck_conn_info['DATABASE'],
        )

    def collect_file_rev_list(self, target_file):
        if not self.git_repo:
            raise ValueError('git repo not initialized')
        try:
            arguments = self.rev_list_params + ['HEAD', target_file]
            shas = self.git_repo.git.rev_list(*arguments).split()

            for sha in shas:
                if sha not in self.envolved_commits_map:
                    self.envolved_commits_map[sha] = True
        except git.exc.GitCommandError as e:
            if e.status == 128:
                logger.info(f'{target_file} not in HEAD, skip')
            else:
                raise e

    def collect_local_commits(self):
        """
        Collect all envolved commits once from the local git repo
        """
        with ThreadPoolExecutor(max_workers=10) as worker:
            futures = []
            for target_file_path in self.__class__.TARGET_FILES.keys():
                futures.append(worker.submit(self.collect_file_rev_list, target_file_path))

            [f.result() for f in futures]

        self.envolved_commits = list(self.envolved_commits_map.keys())

    def init(self):
        repo_name = f'{self.__class__.OWNER}/{self.__class__.REPO}'
        if path.exists(self.local_repo_path):
            logger.info(f'Repo {repo_name} exists, pull origin')
            self.git_repo = git.Repo(self.local_repo_path)
            self.git_repo.remotes.origin.pull()
        else:
            logger.info(f'Repo {repo_name} does not exist, clone from remote')
            makedirs(self.local_repo_path, exist_ok=True)
            self.git_repo = git.Repo.clone_from(self.__class__.ORIGIN, self.local_repo_path)
        logger.info(f'Repo {repo_name} updated to the latest commit')

    def iter_history(self):
        for commit in self.envolved_commits:
            self.take_snapshot(commit)

    def take_snapshot(self, sha):
        commit = self.git_repo.commit(sha)

        for filepath in self.__class__.TARGET_FILES:
            try:
                target_file = commit.tree / filepath
            except KeyError as e:
                logger.info(f'Failed to analyze {filepath} in {sha}, error:{e}')
                continue

            file_content = target_file.data_stream.read().decode('utf-8')
            developer_info = self.get_developer_info(filepath, file_content)
            self.save_snapshot(sha, commit.authored_datetime, filepath, developer_info)

    def get_current_commits(self):
        sql = f'''
        select distinct hexsha from code_owners
        where search_key__owner='{self.__class__.OWNER}' and search_key__repo='{self.__class__.REPO}'
        '''
        result = self.ck_client.execute_no_params(sql)
        current_commits = [item[0] for item in result]
        return current_commits

    def watch(self):
        # Currently all key projects are hosted on GitHub. So the target file's latest history can be
        # analyzed from GitHub web page.
        # TODO Consider: since the repo is already cloned to local storage, is it necessary to parse
        #  github web page? Just pull and get rev list, then compare the list with local database, which
        #  seems to work perfectly.

        # TODO Compare the latest commits from github web page and local database
        # for file in self.__class__.TARGET_FILES:
        #     history_page_url = f'${self.repo_url}/commits/main/{file}'
        #     res = requests.get(history_page_url)
        #     soup = BeautifulSoup(res.content, 'html.parser')
        #     elements = soup.find_all('a', {'class': "Button--secondary Button--small Button text-mono f6"})
        #     for element in elements:
        #         href = element['href']
        #         commit = href.split('/')[-1]
        #         print(commit)
        #         # TODO Assign the diff commits to self.envolved_commits
        #         self.iter_history()

        # By the higher level design, watch() is called after init(), which makes sure the git repo is
        # updated(with pull or a fresh clone)
        current_commits = self.get_current_commits()
        self.collect_local_commits()  # related local commits will be saved to self.envolved_commit
        if current_commits:
            diff = set(self.envolved_commits).difference(set(current_commits))
            self.envolved_commits = diff
            logger.info(f'New code owner info from commits: {diff}')

        self.iter_history()

    def save_snapshot(self, sha, authored_date, filepath, developer_info):
        # Implemented in concrete classes
        rows = []
        for d in developer_info:
            row = {
                'search_key__owner': self.__class__.OWNER,
                'search_key__repo': self.__class__.REPO,
                'search_key__origin': self.__class__.ORIGIN,
                'hexsha': sha,
                'authored_date': authored_date,
                'filepath': filepath,
            }
            self.apply_row_info(row, filepath, d)

            rows.append(row)
            if len(rows) >= self.__class__.BATCH_SIZE:
                self.ck_client.execute('insert into table code_owners values', rows)

        if rows:
            self.ck_client.execute('insert into table code_owners values', rows)

    def get_developer_info(self, file_path, file_content):
        pass

    def apply_row_info(self, row, filepath, developer):
        pass


class LLVMCodeOwnerWatcher(CodeOwnerWatcher):
    TARGET_FILES = {
        "lldb/CodeOwners.rst": True,
        "clang/CodeOwners.rst": True,
        # "clang/docs/CodeOwners.rst": True,  # ref to clang/CodeOwners.rst, skip it
        "libcxxabi/CREDITS.TXT": True,
        "llvm/CREDITS.TXT": True,
        "compiler-rt/CODE_OWNERS.TXT": True,
        "libclc/CREDITS.TXT": True,
        "libcxx/CREDITS.TXT": True,
        "clang-tools-extra/CODE_OWNERS.TXT": True,
        "flang/CODE_OWNERS.TXT": True,
        "lldb/CODE_OWNERS.txt": True,
        "llvm/CODE_OWNERS.TXT": True,
        "lld/CODE_OWNERS.TXT": True,
        "bolt/CODE_OWNERS.TXT": True,
        "pstl/CREDITS.txt": True,
        "polly/CREDITS.txt": True,
        ".github/CODEOWNERS": True,
        "compiler-rt/CREDITS.TXT": True,
        "libcxx/utils/ci/BOT_OWNERS.txt": True,
        "openmp/CREDITS.txt": True,
    }

    OWNER = 'llvm'
    REPO = 'llvm-project'
    ORIGIN = 'https://github.com/llvm/llvm-project'

    KEY_MAP = {
        'N': 'name',
        'H': 'phabricator_handle',
        'E': 'email',
        'D': 'description'
    }

    def get_developer_info(self, file_path: str, content):
        if str.lower(file_path).endswith('.rst'):
            return LLVMCodeOwnerWatcher.get_rst_developer_info(content)
        return LLVMCodeOwnerWatcher.get_txt_developer_info(content)

    @staticmethod
    def parse_info(self, info_line):
        parts = map(lambda x: x.strip(), info_line.replace('\\@', '@').split(','))
        structured_info = {}
        for part in parts:
            matches = re.findall('(\w.*?)\s*\((.*?)\)', part, re.DOTALL)
            value, key = matches[0]
            key = str.lower(key)
            structured_info[key] = value

        return structured_info

    @staticmethod
    def parse_rst(text: str) -> docutils.nodes.document:
        parser = docutils.parsers.rst.Parser()
        components = (docutils.parsers.rst.Parser,)
        settings = docutils.frontend.OptionParser(components=components).get_default_values()
        document = docutils.utils.new_document('<rst-doc>', settings=settings)
        parser.parse(text, document)
        return document

    @staticmethod
    def get_rst_developer_info(self, content):
        developers = []

        doc = LLVMCodeOwnerWatcher.parse_rst(content)
        for doc_child in doc.children:
            for child in doc_child.children:
                if type(child) is docutils.nodes.section:
                    for node in child.children:
                        if type(node) is docutils.nodes.title:
                            continue
                        if type(node) is docutils.nodes.section:
                            current_module = ''

                            for c in node.children:
                                if type(c) is docutils.nodes.paragraph:
                                    continue

                                if type(c) is docutils.nodes.title:
                                    current_module = c.rawsource
                                elif type(c) is docutils.nodes.line_block:
                                    name = c.children[0].rawsource
                                    info = c.children[1].rawsource
                                    info_dict = LLVMCodeOwnerWatcher.parse_info(info)
                                    info_dict['name'] = name
                                    info_dict['module'] = current_module
                                    developers.append(info_dict)
                                elif type(c) is docutils.nodes.section:
                                    current_module = c[0].rawsource
                                    for cc in c[1:]:
                                        name = cc[0].rawsource
                                        info = cc[1].rawsource
                                        info_dict = LLVMCodeOwnerWatcher.parse_info(info)
                                        info_dict['name'] = name
                                        info_dict['module'] = current_module
                                        developers.append(info_dict)

        return developers

    @staticmethod
    def get_txt_developer_info(self, content):
        all_developers = []  # All developers of diff modules in the current file
        current_developers = []
        for raw_line in content.split('\n'):
            line = raw_line.strip()
            if re.search('[NEHD]:\ ', line):
                if line.startswith('N:'):
                    # Append the previously stored developer info
                    # and move to the next one
                    if current_developers:
                        all_developers += current_developers

                    names = map(str.strip, line.split(':')[-1].split(','))
                    current_developers = [{'name': n} for n in names]
                elif line.startswith('E:'):
                    emails = list(map(str.strip, line.split(':')[-1].split(',')))
                    key = LLVMCodeOwnerWatcher.KEY_MAP['E']
                    try:
                        for index, email in enumerate(emails):
                            current_developers[index][key] = email
                    except IndexError as e:
                        logger.warning('Length of emails and developers are not the same, it is likely that one '
                                       'developer has multiple emails')
                        logger.info(f'Developers: {current_developers}, emails: {emails}')
                else:
                    parts = list(map(str.strip, line.split(':')))
                    key = LLVMCodeOwnerWatcher.KEY_MAP[parts[0]]
                    val = parts[1]
                    for d in current_developers:
                        d[key] = val

        # Don't forget the last developers
        if current_developers:
            all_developers += current_developers

        return all_developers

    def apply_row_info(self, row, filepath, developer):
        # Another possible condition control is to check the filepath with .rst suffix
        row['module'] = developer['module'] if 'module' in developer else filepath
        row['name'] = developer.get('name') or ''
        row['email'] = developer.get('email') or ''
        row['github_login'] = ''
        row['misc'] = json.dumps({'description': developer['description']}) if 'description' in developer else ''


class PytorchCodeOwnerWatcher(CodeOwnerWatcher):
    TARGET_FILES = {
        'CODEOWNERS': True,
    }
    OWNER = 'pytorch'
    REPO = 'pytorch'
    ORIGIN = 'https://github.com/pytorch/pytorch'

    @staticmethod
    def analyze_module_line(line):
        parts = line.split()
        module_path = parts[0]
        developers = list(map(lambda d: d.strip().replace('@', ''), parts[1:]))
        return module_path, developers

    def get_developer_info(self, filepath, code_owner_content):
        desc = ''
        analyzing_modules = False

        developer_info = []

        for line in code_owner_content.split('\n'):
            content = line.strip()

            if not content:
                desc = ''
                analyzing_modules = False
            else:
                if content.startswith('#'):
                    if analyzing_modules:
                        # Exit analyzing module state
                        desc = content + '\n'
                        analyzing_modules = False
                    else:
                        desc += content + '\n'
                else:
                    analyzing_modules = True
                    module_path, developers = PytorchCodeOwnerWatcher.analyze_module_line(content)
                    for developer_github_login in developers:
                        developer_info.append((desc, module_path, developer_github_login))

        return developer_info

    def apply_row_info(self, row, filepath, developer_tup):
        desc, module_path, github_login = developer_tup

        row['module'] = module_path
        row['name'] = ''
        row['email'] = ''
        row['github_login'] = github_login
        row['misc'] = json.dumps({'description': desc})


class KernelCodeOwnerWatcher(CodeOwnerWatcher):
    TARGET_FILES = {
        'MAINTAINERS': True,
        'CREDITS': True,
    }

    OWNER = 'torvalds'
    REPO = 'linux'
    ORIGIN = 'https://github.com/torvalds/linux'

    # @staticmethod
    # def analyze_module_line(line):
    #     parts = line.split()
    #     module_path = parts[0]
    #     developers = list(map(lambda d: d.strip().replace('@', ''), parts[1:]))
    #     return module_path, developers

    @staticmethod
    def get_name_email(line):
        if '<' not in line:
            email = line.strip()
            return '', email

        parts = line.replace('M:', '').strip().split('<')
        name = parts[0]
        try:
            email = parts[1][:-1]
        except IndexError as e:
            print(e)
        return name, email

    @staticmethod
    def get_owner_info_from_maintainers(file_path, file_content):
        splitter = '''Maintainers List\n----------------'''
        parts = file_content.split(splitter)
        maintainer_blocks = parts[1].split('\n\n')[2:]
        maintainer_infos = []

        for maintainer_block in maintainer_blocks:
            lines = maintainer_block.split('\n')
            module = lines[0].strip()
            maintainers = []
            reviewers = []
            files = []

            for line in lines[1:]:
                if line.startswith('M:'):
                    name, email = KernelCodeOwnerWatcher.get_name_email(line)
                    maintainers.append((name, email))
                    # parts = line.split()
                    # print(parts)
                elif line.startswith('R:'):
                    name, email = KernelCodeOwnerWatcher.get_name_email(line)
                    reviewers.append((name, email))
                elif line.startswith('F:'):
                    file_path = line.replace('F:', '').strip()
                    files.append(file_path)
            for name, email in maintainers:
                maintainer_infos.append((module, name, email, 'maintainer'))
            for name, email in reviewers:
                maintainer_infos.append((module, name, email, 'reviewer'))
            # maintainer_infos.append((module, maintainers, reviewers))

        return maintainer_infos

    def get_developer_info(self, filepath, code_owner_content):
        if filepath == 'MAINTAINERS':
            return KernelCodeOwnerWatcher.get_owner_info_from_maintainers(filepath, code_owner_content)
        elif filepath == 'CREDITS':
            return []

    def apply_row_info(self, row, filepath, developer_tup):
        module, name, email, owner_type = developer_tup
        row['module'] = module
        row['name'] = name
        row['email'] = email
        row['misc'] = json.dumps({'owner_type': owner_type})
        row['github_login'] = ''
