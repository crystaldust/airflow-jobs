import json
import re
from concurrent.futures import ThreadPoolExecutor
from os import path, makedirs

import docutils.frontend
import docutils.nodes
import docutils.parsers.rst
import docutils.utils
import git
import yaml

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
        self.file_commits_map = {}  # Not used yet for now, just for debugging
        self.envolved_commits = []
        self.ck_client = CKServer(
            host=ck_conn_info['HOST'],
            port=ck_conn_info['PORT'],
            user=ck_conn_info['USER'],
            password=ck_conn_info['PASSWD'],
            database=ck_conn_info['DATABASE'],
        )

    def collect_file_rev_list(self, target_file):
        self.file_commits_map[target_file] = []
        if not self.git_repo:
            raise ValueError('git repo not initialized')
        try:
            arguments = self.rev_list_params + ['HEAD', target_file]
            shas = self.git_repo.git.rev_list(*arguments).split()

            for sha in shas:
                self.file_commits_map[target_file].append(sha)
                if sha not in self.envolved_commits_map:
                    self.envolved_commits_map[sha] = [target_file]
                else:
                    self.envolved_commits_map[sha].append(target_file)
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
        logger.debug(f'[DEBUG] handling commit {sha}')
        for filepath in self.envolved_commits_map[sha]:
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


class K8SCodeOwnerWatcher(CodeOwnerWatcher):
    TARGET_FILES = {
        'staging/OWNERS': True,
        'staging/publishing/OWNERS': True,
        'staging/src/k8s.io/cloud-provider/OWNERS': True,
        'staging/src/k8s.io/cloud-provider/controllers/service/OWNERS': True,
        'staging/src/k8s.io/cloud-provider/controllers/service/config/OWNERS': True,
        'staging/src/k8s.io/cloud-provider/controllers/route/OWNERS': True,
        'staging/src/k8s.io/cloud-provider/controllers/nodelifecycle/OWNERS': True,
        'staging/src/k8s.io/client-go/OWNERS': True,
        'staging/src/k8s.io/client-go/util/keyutil/OWNERS': True,
        'staging/src/k8s.io/client-go/util/retry/OWNERS': True,
        'staging/src/k8s.io/client-go/util/cert/OWNERS': True,
        'staging/src/k8s.io/client-go/util/certificate/OWNERS': True,
        'staging/src/k8s.io/client-go/util/csaupgrade/OWNERS': True,
        'staging/src/k8s.io/client-go/plugin/pkg/client/auth/OWNERS': True,
        'staging/src/k8s.io/client-go/transport/OWNERS': True,
        'staging/src/k8s.io/client-go/listers/rbac/OWNERS': True,
        'staging/src/k8s.io/client-go/openapi/OWNERS': True,
        'staging/src/k8s.io/client-go/rest/OWNERS': True,
        'staging/src/k8s.io/client-go/tools/remotecommand/OWNERS': True,
        'staging/src/k8s.io/client-go/tools/record/OWNERS': True,
        'staging/src/k8s.io/client-go/tools/leaderelection/OWNERS': True,
        'staging/src/k8s.io/client-go/tools/cache/OWNERS': True,
        'staging/src/k8s.io/client-go/tools/portforward/OWNERS': True,
        'staging/src/k8s.io/client-go/tools/auth/OWNERS': True,
        'staging/src/k8s.io/client-go/tools/metrics/OWNERS': True,
        'staging/src/k8s.io/client-go/tools/events/OWNERS': True,
        'staging/src/k8s.io/client-go/applyconfigurations/OWNERS': True,
        'staging/src/k8s.io/client-go/kubernetes/typed/authorization/OWNERS': True,
        'staging/src/k8s.io/client-go/kubernetes/typed/rbac/OWNERS': True,
        'staging/src/k8s.io/client-go/kubernetes/typed/authentication/OWNERS': True,
        'staging/src/k8s.io/client-go/pkg/apis/clientauthentication/OWNERS': True,
        'staging/src/k8s.io/csi-translation-lib/OWNERS': True,
        'staging/src/k8s.io/mount-utils/OWNERS': True,
        'staging/src/k8s.io/dynamic-resource-allocation/OWNERS': True,
        'staging/src/k8s.io/kube-proxy/OWNERS': True,
        'staging/src/k8s.io/kube-proxy/config/OWNERS': True,
        'staging/src/k8s.io/endpointslice/OWNERS': True,
        'staging/src/k8s.io/sample-cli-plugin/OWNERS': True,
        'staging/src/k8s.io/cri-client/OWNERS': True,
        'staging/src/k8s.io/cluster-bootstrap/OWNERS': True,
        'staging/src/k8s.io/cluster-bootstrap/token/OWNERS': True,
        'staging/src/k8s.io/sample-controller/OWNERS': True,
        'staging/src/k8s.io/apiserver/OWNERS': True,
        'staging/src/k8s.io/apiserver/plugin/pkg/audit/OWNERS': True,
        'staging/src/k8s.io/apiserver/plugin/pkg/authenticator/OWNERS': True,
        'staging/src/k8s.io/apiserver/plugin/pkg/authorizer/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/server/options/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/server/options/encryptionconfig/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/server/mux/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/server/routes/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/server/filters/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/authorization/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/util/flowcontrol/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/admission/plugin/policy/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/admission/plugin/resourcequota/apis/resourcequota/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/admission/plugin/cel/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/storageversion/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/quota/v1/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/quota/v1/generic/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/endpoints/handlers/metrics/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/endpoints/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/endpoints/discovery/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/endpoints/request/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/endpoints/metrics/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/endpoints/testing/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/endpoints/filters/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/audit/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/features/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/storage/cacher/metrics/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/storage/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/storage/value/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/storage/value/encrypt/envelope/kmsv2/v2/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/storage/storagebackend/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/storage/etcd3/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/storage/etcd3/metrics/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/storage/testing/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/authentication/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/authentication/request/x509/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/cel/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/registry/generic/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/registry/rest/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/apis/OWNERS': True,
        'staging/src/k8s.io/apiserver/pkg/apis/audit/OWNERS': True,
        'staging/src/k8s.io/kubelet/OWNERS': True,
        'staging/src/k8s.io/kubelet/config/OWNERS': True,
        'staging/src/k8s.io/kubelet/pkg/apis/deviceplugin/OWNERS': True,
        'staging/src/k8s.io/kubelet/pkg/apis/dra/OWNERS': True,
        'staging/src/k8s.io/kubelet/pkg/apis/credentialprovider/OWNERS': True,
        'staging/src/k8s.io/kube-scheduler/OWNERS': True,
        'staging/src/k8s.io/kube-scheduler/config/OWNERS': True,
        'staging/src/k8s.io/kube-scheduler/extender/OWNERS': True,
        'staging/src/k8s.io/kms/OWNERS': True,
        'staging/src/k8s.io/kms/apis/OWNERS': True,
        'staging/src/k8s.io/api/authorization/OWNERS': True,
        'staging/src/k8s.io/api/OWNERS': True,
        'staging/src/k8s.io/api/extensions/OWNERS': True,
        'staging/src/k8s.io/api/resource/OWNERS': True,
        'staging/src/k8s.io/api/rbac/OWNERS': True,
        'staging/src/k8s.io/api/autoscaling/OWNERS': True,
        'staging/src/k8s.io/api/batch/OWNERS': True,
        'staging/src/k8s.io/api/apps/OWNERS': True,
        'staging/src/k8s.io/api/networking/OWNERS': True,
        'staging/src/k8s.io/api/policy/OWNERS': True,
        'staging/src/k8s.io/api/certificates/OWNERS': True,
        'staging/src/k8s.io/api/imagepolicy/OWNERS': True,
        'staging/src/k8s.io/api/storage/OWNERS': True,
        'staging/src/k8s.io/api/node/OWNERS': True,
        'staging/src/k8s.io/api/authentication/OWNERS': True,
        'staging/src/k8s.io/apiextensions-apiserver/OWNERS': True,
        'staging/src/k8s.io/apiextensions-apiserver/pkg/features/OWNERS': True,
        'staging/src/k8s.io/apiextensions-apiserver/pkg/apis/OWNERS': True,
        'staging/src/k8s.io/metrics/OWNERS': True,
        'staging/src/k8s.io/metrics/pkg/apis/OWNERS': True,
        'staging/src/k8s.io/apimachinery/OWNERS': True,
        'staging/src/k8s.io/apimachinery/third_party/forked/golang/json/OWNERS': True,
        'staging/src/k8s.io/apimachinery/pkg/OWNERS': True,
        'staging/src/k8s.io/apimachinery/pkg/util/OWNERS': True,
        'staging/src/k8s.io/apimachinery/pkg/util/mergepatch/OWNERS': True,
        'staging/src/k8s.io/apimachinery/pkg/util/strategicpatch/OWNERS': True,
        'staging/src/k8s.io/apimachinery/pkg/util/validation/OWNERS': True,
        'staging/src/k8s.io/apimachinery/pkg/api/resource/OWNERS': True,
        'staging/src/k8s.io/apimachinery/pkg/api/errors/OWNERS': True,
        'staging/src/k8s.io/apimachinery/pkg/api/meta/OWNERS': True,
        'staging/src/k8s.io/apimachinery/pkg/api/validation/OWNERS': True,
        'staging/src/k8s.io/apimachinery/pkg/apis/OWNERS': True,
        'staging/src/k8s.io/apimachinery/pkg/apis/meta/v1/OWNERS': True,
        'staging/src/k8s.io/kube-aggregator/OWNERS': True,
        'staging/src/k8s.io/kube-aggregator/pkg/registry/apiservice/etcd/OWNERS': True,
        'staging/src/k8s.io/kube-aggregator/pkg/apis/OWNERS': True,
        'staging/src/k8s.io/component-base/OWNERS': True,
        'staging/src/k8s.io/component-base/featuregate/OWNERS': True,
        'staging/src/k8s.io/component-base/config/OWNERS': True,
        'staging/src/k8s.io/component-base/version/OWNERS': True,
        'staging/src/k8s.io/component-base/term/OWNERS': True,
        'staging/src/k8s.io/component-base/metrics/OWNERS': True,
        'staging/src/k8s.io/component-base/logs/OWNERS': True,
        'staging/src/k8s.io/component-base/logs/api/OWNERS': True,
        'staging/src/k8s.io/component-base/tracing/OWNERS': True,
        'staging/src/k8s.io/component-base/tracing/api/OWNERS': True,
        'staging/src/k8s.io/component-base/codec/OWNERS': True,
        'staging/src/k8s.io/component-base/cli/OWNERS': True,
        'staging/src/k8s.io/component-base/configz/OWNERS': True,
        'staging/src/k8s.io/component-helpers/OWNERS': True,
        'staging/src/k8s.io/component-helpers/auth/OWNERS': True,
        'staging/src/k8s.io/component-helpers/apps/OWNERS': True,
        'staging/src/k8s.io/component-helpers/apimachinery/OWNERS': True,
        'staging/src/k8s.io/component-helpers/storage/OWNERS': True,
        'staging/src/k8s.io/component-helpers/scheduling/OWNERS': True,
        'staging/src/k8s.io/component-helpers/node/OWNERS': True,
        'staging/src/k8s.io/code-generator/OWNERS': True,
        'staging/src/k8s.io/code-generator/cmd/client-gen/OWNERS': True,
        'staging/src/k8s.io/code-generator/cmd/go-to-protobuf/OWNERS': True,
        'staging/src/k8s.io/kubectl/OWNERS': True,
        'staging/src/k8s.io/kubectl/pkg/util/i18n/translations/OWNERS': True,
        'staging/src/k8s.io/kubectl/pkg/util/i18n/translations/kubectl/OWNERS': True,
        'staging/src/k8s.io/kubectl/pkg/util/openapi/OWNERS': True,
        'staging/src/k8s.io/kubectl/pkg/explain/OWNERS': True,
        'staging/src/k8s.io/kubectl/pkg/cmd/auth/OWNERS': True,
        'staging/src/k8s.io/kube-controller-manager/OWNERS': True,
        'staging/src/k8s.io/kube-controller-manager/config/OWNERS': True,
        'staging/src/k8s.io/controller-manager/OWNERS': True,
        'staging/src/k8s.io/controller-manager/config/OWNERS': True,
        'staging/src/k8s.io/controller-manager/pkg/features/OWNERS': True,
        'staging/src/k8s.io/sample-apiserver/OWNERS': True,
        'staging/src/k8s.io/cli-runtime/OWNERS': True,
        'staging/src/k8s.io/pod-security-admission/OWNERS': True,
        'staging/src/k8s.io/cri-api/OWNERS': True,
        'OWNERS': True,
        'third_party/forked/shell2junit/OWNERS': True,
        'third_party/OWNERS': True,
        'plugin/OWNERS': True,
        'plugin/pkg/admission/OWNERS': True,
        'plugin/pkg/admission/runtimeclass/OWNERS': True,
        'plugin/pkg/admission/noderestriction/OWNERS': True,
        'plugin/pkg/admission/certificates/OWNERS': True,
        'plugin/pkg/admission/serviceaccount/OWNERS': True,
        'plugin/pkg/admission/imagepolicy/OWNERS': True,
        'plugin/pkg/admission/podtolerationrestriction/apis/podtolerationrestriction/OWNERS': True,
        'plugin/pkg/admission/eventratelimit/apis/eventratelimit/OWNERS': True,
        'plugin/pkg/auth/OWNERS': True,
        'plugin/pkg/auth/authenticator/OWNERS': True,
        'plugin/pkg/auth/authorizer/OWNERS': True,
        'plugin/pkg/auth/authorizer/node/OWNERS': True,
        'OWNERS_ALIASES': True,
        'logo/OWNERS': True,
        '.github/OWNERS': True,
        'vendor/OWNERS': True,
        'vendor/github.com/Microsoft/go-winio/CODEOWNERS': True,
        'vendor/github.com/Microsoft/hcsshim/CODEOWNERS': True,
        'vendor/go.opentelemetry.io/otel/CODEOWNERS': True,
        'vendor/sigs.k8s.io/knftables/OWNERS': True,
        'vendor/sigs.k8s.io/json/OWNERS': True,
        'vendor/sigs.k8s.io/yaml/OWNERS': True,
        'vendor/sigs.k8s.io/yaml/goyaml.v3/OWNERS': True,
        'vendor/sigs.k8s.io/yaml/goyaml.v2/OWNERS': True,
        'vendor/go.etcd.io/etcd/raft/v3/OWNERS': True,
        'vendor/k8s.io/kube-openapi/pkg/util/proto/OWNERS': True,
        'vendor/k8s.io/kube-openapi/pkg/generators/rules/OWNERS': True,
        'vendor/k8s.io/klog/v2/OWNERS': True,
        'vendor/k8s.io/utils/nsenter/OWNERS': True,
        'vendor/k8s.io/utils/pointer/OWNERS': True,
        'vendor/k8s.io/utils/cpuset/OWNERS': True,
        'vendor/k8s.io/utils/ptr/OWNERS': True,
        'CHANGELOG/OWNERS': True,
        'api/OWNERS': True,
        'LICENSES/OWNERS': True,
        'cmd/genman/OWNERS': True,
        'cmd/prune-junit-xml/OWNERS': True,
        'cmd/kubeadm/OWNERS': True,
        'cmd/cloud-controller-manager/OWNERS': True,
        'cmd/OWNERS': True,
        'cmd/kube-proxy/OWNERS': True,
        'cmd/kube-apiserver/OWNERS': True,
        'cmd/dependencycheck/OWNERS': True,
        'cmd/genyaml/OWNERS': True,
        'cmd/clicheck/OWNERS': True,
        'cmd/preferredimports/OWNERS': True,
        'cmd/kubectl-convert/OWNERS': True,
        'cmd/kubelet/app/OWNERS': True,
        'cmd/kubelet/OWNERS': True,
        'cmd/kube-scheduler/OWNERS': True,
        'cmd/kubemark/OWNERS': True,
        'cmd/gendocs/OWNERS': True,
        'cmd/importverifier/OWNERS': True,
        'cmd/dependencyverifier/OWNERS': True,
        'cmd/kubectl/OWNERS': True,
        'cmd/gotemplate/OWNERS': True,
        'cmd/kube-controller-manager/OWNERS': True,
        'cmd/yamlfmt/OWNERS': True,
        'cluster/addons/OWNERS': True,
        'cluster/addons/kube-proxy/OWNERS': True,
        'cluster/addons/ip-masq-agent/OWNERS': True,
        'cluster/addons/cluster-loadbalancing/OWNERS': True,
        'cluster/addons/metadata-agent/OWNERS': True,
        'cluster/addons/node-problem-detector/OWNERS': True,
        'cluster/addons/kube-network-policies/OWNERS': True,
        'cluster/addons/addon-manager/OWNERS': True,
        'cluster/addons/metadata-proxy/OWNERS': True,
        'cluster/addons/dns-horizontal-autoscaler/OWNERS': True,
        'cluster/addons/dns/OWNERS': True,
        'cluster/addons/volumesnapshots/OWNERS': True,
        'cluster/addons/metrics-server/OWNERS': True,
        'cluster/addons/calico-policy-controller/OWNERS': True,
        'cluster/addons/fluentd-gcp/OWNERS': True,
        'cluster/images/OWNERS': True,
        'cluster/images/etcd/OWNERS': True,
        'cluster/images/kubemark/OWNERS': True,
        'cluster/images/etcd-version-monitor/OWNERS': True,
        'cluster/OWNERS': True,
        'cluster/log-dump/OWNERS': True,
        'cluster/skeleton/OWNERS': True,
        'cluster/pre-existing/OWNERS': True,
        'cluster/kubemark/OWNERS': True,
        'cluster/gce/OWNERS': True,
        'cluster/gce/gci/OWNERS': True,
        'cluster/gce/windows/OWNERS': True,
        'cluster/gce/manifests/OWNERS': True,
        'docs/OWNERS': True,
        'test/images/nonewprivs/OWNERS': True,
        'test/images/jessie-dnsutils/OWNERS': True,
        'test/images/OWNERS': True,
        'test/images/nautilus/OWNERS': True,
        'test/images/apparmor-loader/OWNERS': True,
        'test/images/regression-issue-74839/OWNERS': True,
        'test/images/busybox/OWNERS': True,
        'test/images/cuda-vector-add-old/OWNERS': True,
        'test/images/kitten/OWNERS': True,
        'test/images/cuda-vector-add/OWNERS': True,
        'test/images/agnhost/OWNERS': True,
        'test/images/volume/OWNERS': True,
        'test/images/sample-apiserver/OWNERS': True,
        'test/images/nonroot/OWNERS': True,
        'test/images/redis/OWNERS': True,
        'test/conformance/OWNERS': True,
        'test/conformance/testdata/OWNERS': True,
        'test/conformance/image/OWNERS': True,
        'test/OWNERS': True,
        'test/e2e/feature/OWNERS': True,
        'test/e2e/architecture/OWNERS': True,
        'test/e2e/common/OWNERS': True,
        'test/e2e/common/network/OWNERS': True,
        'test/e2e/common/storage/OWNERS': True,
        'test/e2e/common/node/OWNERS': True,
        'test/e2e/autoscaling/OWNERS': True,
        'test/e2e/framework/OWNERS': True,
        'test/e2e/framework/providers/vsphere/OWNERS': True,
        'test/e2e/framework/autoscaling/OWNERS': True,
        'test/e2e/framework/metrics/OWNERS': True,
        'test/e2e/framework/volume/OWNERS': True,
        'test/e2e/auth/OWNERS': True,
        'test/e2e/apps/OWNERS': True,
        'test/e2e/dra/OWNERS': True,
        'test/e2e/network/OWNERS': True,
        'test/e2e/network/netpol/OWNERS': True,
        'test/e2e/windows/OWNERS': True,
        'test/e2e/apimachinery/OWNERS': True,
        'test/e2e/testing-manifests/auth/encrypt/OWNERS': True,
        'test/e2e/testing-manifests/statefulset/etcd/OWNERS': True,
        'test/e2e/testing-manifests/dra/OWNERS': True,
        'test/e2e/testing-manifests/storage-csi/OWNERS': True,
        'test/e2e/storage/OWNERS': True,
        'test/e2e/scheduling/OWNERS': True,
        'test/e2e/kubectl/OWNERS': True,
        'test/e2e/upgrades/autoscaling/OWNERS': True,
        'test/e2e/upgrades/apps/OWNERS': True,
        'test/e2e/node/OWNERS': True,
        'test/e2e/cloud/OWNERS': True,
        'test/e2e/cloud/gcp/OWNERS': True,
        'test/e2e/instrumentation/OWNERS': True,
        'test/e2e/instrumentation/logging/OWNERS': True,
        'test/e2e/lifecycle/OWNERS': True,
        'test/fuzz/OWNERS': True,
        'test/fixtures/pkg/kubectl/OWNERS': True,
        'test/typecheck/OWNERS': True,
        'test/integration/podgc/OWNERS': True,
        'test/integration/pods/OWNERS': True,
        'test/integration/etcd/OWNERS': True,
        'test/integration/job/OWNERS': True,
        'test/integration/framework/OWNERS': True,
        'test/integration/scheduler_perf/OWNERS': True,
        'test/integration/apiserver/OWNERS': True,
        'test/integration/apiserver/tracing/OWNERS': True,
        'test/integration/metrics/OWNERS': True,
        'test/integration/logs/OWNERS': True,
        'test/integration/daemonset/OWNERS': True,
        'test/integration/scheduler/OWNERS': True,
        'test/integration/dryrun/OWNERS': True,
        'test/integration/node/OWNERS': True,
        'test/integration/deployment/OWNERS': True,
        'test/kubemark/OWNERS': True,
        'test/cmd/OWNERS': True,
        'test/e2e_kubeadm/OWNERS': True,
        'test/utils/image/OWNERS': True,
        'test/e2e_node/OWNERS': True,
        'test/e2e_node/jenkins/OWNERS': True,
        'test/e2e_node/perf/workloads/OWNERS': True,
        'test/instrumentation/OWNERS': True,
        'test/instrumentation/testdata/OWNERS': True,
        'build/pause/OWNERS': True,
        'build/OWNERS': True,
        'build/build-image/OWNERS': True,
        'hack/OWNERS': True,
        'hack/testdata/OWNERS': True,
        'hack/jenkins/OWNERS': True,
        'hack/verify-flags/OWNERS': True,
        'pkg/OWNERS': True,
        'pkg/proxy/ipvs/OWNERS': True,
        'pkg/proxy/OWNERS': True,
        'pkg/proxy/config/OWNERS': True,
        'pkg/proxy/kubemark/OWNERS': True,
        'pkg/proxy/winkernel/OWNERS': True,
        'pkg/proxy/apis/config/OWNERS': True,
        'pkg/proxy/iptables/OWNERS': True,
        'pkg/util/removeall/OWNERS': True,
        'pkg/util/goroutinemap/OWNERS': True,
        'pkg/util/kernel/OWNERS': True,
        'pkg/util/coverage/OWNERS': True,
        'pkg/util/bandwidth/OWNERS': True,
        'pkg/util/iptables/OWNERS': True,
        'pkg/controlplane/OWNERS': True,
        'pkg/controlplane/storageversionhashdata/OWNERS': True,
        'pkg/generated/openapi/OWNERS': True,
        'pkg/auth/OWNERS': True,
        'pkg/quota/v1/OWNERS': True,
        'pkg/quota/v1/evaluator/OWNERS': True,
        'pkg/quota/v1/install/OWNERS': True,
        'pkg/credentialprovider/OWNERS': True,
        'pkg/security/apparmor/OWNERS': True,
        'pkg/kubelet/server/OWNERS': True,
        'pkg/kubelet/OWNERS': True,
        'pkg/kubelet/stats/OWNERS': True,
        'pkg/kubelet/kubeletconfig/OWNERS': True,
        'pkg/kubelet/prober/OWNERS': True,
        'pkg/kubelet/network/OWNERS': True,
        'pkg/kubelet/network/dns/OWNERS': True,
        'pkg/kubelet/pluginmanager/OWNERS': True,
        'pkg/kubelet/cm/OWNERS': True,
        'pkg/kubelet/cm/cpumanager/OWNERS': True,
        'pkg/kubelet/cm/topologymanager/OWNERS': True,
        'pkg/kubelet/cm/devicemanager/OWNERS': True,
        'pkg/kubelet/metrics/OWNERS': True,
        'pkg/kubelet/certificate/OWNERS': True,
        'pkg/kubelet/volumemanager/OWNERS': True,
        'pkg/kubelet/apis/config/OWNERS': True,
        'pkg/kubelet/token/OWNERS': True,
        'pkg/features/OWNERS': True,
        'pkg/kubemark/OWNERS': True,
        'pkg/controller/cronjob/OWNERS': True,
        'pkg/controller/cronjob/config/OWNERS': True,
        'pkg/controller/daemon/OWNERS': True,
        'pkg/controller/daemon/config/OWNERS': True,
        'pkg/controller/history/OWNERS': True,
        'pkg/controller/podgc/OWNERS': True,
        'pkg/controller/podgc/config/OWNERS': True,
        'pkg/controller/OWNERS': True,
        'pkg/controller/ttlafterfinished/config/OWNERS': True,
        'pkg/controller/util/endpointslice/OWNERS': True,
        'pkg/controller/job/OWNERS': True,
        'pkg/controller/job/config/OWNERS': True,
        'pkg/controller/endpointslice/OWNERS': True,
        'pkg/controller/podautoscaler/OWNERS': True,
        'pkg/controller/podautoscaler/config/OWNERS': True,
        'pkg/controller/garbagecollector/OWNERS': True,
        'pkg/controller/garbagecollector/config/OWNERS': True,
        'pkg/controller/statefulset/OWNERS': True,
        'pkg/controller/statefulset/config/OWNERS': True,
        'pkg/controller/endpointslicemirroring/OWNERS': True,
        'pkg/controller/certificates/OWNERS': True,
        'pkg/controller/certificates/approver/OWNERS': True,
        'pkg/controller/serviceaccount/OWNERS': True,
        'pkg/controller/resourcequota/OWNERS': True,
        'pkg/controller/resourcequota/config/OWNERS': True,
        'pkg/controller/endpoint/OWNERS': True,
        'pkg/controller/endpoint/config/OWNERS': True,
        'pkg/controller/validatingadmissionpolicystatus/OWNERS': True,
        'pkg/controller/tainteviction/OWNERS': True,
        'pkg/controller/volume/OWNERS': True,
        'pkg/controller/volume/ephemeral/OWNERS': True,
        'pkg/controller/replicaset/OWNERS': True,
        'pkg/controller/replicaset/config/OWNERS': True,
        'pkg/controller/disruption/OWNERS': True,
        'pkg/controller/resourceclaim/OWNERS': True,
        'pkg/controller/resourceclaim/metrics/OWNERS': True,
        'pkg/controller/namespace/OWNERS': True,
        'pkg/controller/namespace/config/OWNERS': True,
        'pkg/controller/nodeipam/OWNERS': True,
        'pkg/controller/nodeipam/config/OWNERS': True,
        'pkg/controller/nodeipam/ipam/OWNERS': True,
        'pkg/controller/apis/config/OWNERS': True,
        'pkg/controller/nodelifecycle/OWNERS': True,
        'pkg/controller/nodelifecycle/config/OWNERS': True,
        'pkg/controller/nodelifecycle/scheduler/OWNERS': True,
        'pkg/controller/replication/OWNERS': True,
        'pkg/controller/replication/config/OWNERS': True,
        'pkg/controller/deployment/OWNERS': True,
        'pkg/controller/deployment/config/OWNERS': True,
        'pkg/api/v1/OWNERS': True,
        'pkg/api/OWNERS': True,
        'pkg/api/service/OWNERS': True,
        'pkg/api/pod/OWNERS': True,
        'pkg/api/persistentvolumeclaim/OWNERS': True,
        'pkg/api/testing/OWNERS': True,
        'pkg/serviceaccount/OWNERS': True,
        'pkg/kubeapiserver/OWNERS': True,
        'pkg/kubeapiserver/authenticator/OWNERS': True,
        'pkg/kubeapiserver/authorizer/OWNERS': True,
        'pkg/volume/iscsi/OWNERS': True,
        'pkg/volume/OWNERS': True,
        'pkg/volume/flexvolume/OWNERS': True,
        'pkg/volume/nfs/OWNERS': True,
        'pkg/volume/util/subpath/OWNERS': True,
        'pkg/volume/git_repo/OWNERS': True,
        'pkg/volume/portworx/OWNERS': True,
        'pkg/volume/configmap/OWNERS': True,
        'pkg/volume/fc/OWNERS': True,
        'pkg/volume/secret/OWNERS': True,
        'pkg/volume/downwardapi/OWNERS': True,
        'pkg/volume/emptydir/OWNERS': True,
        'pkg/kubectl/OWNERS': True,
        'pkg/scheduler/OWNERS': True,
        'pkg/scheduler/framework/plugins/dynamicresources/OWNERS': True,
        'pkg/scheduler/framework/plugins/nodevolumelimits/OWNERS': True,
        'pkg/scheduler/framework/plugins/volumerestrictions/OWNERS': True,
        'pkg/scheduler/framework/plugins/volumezone/OWNERS': True,
        'pkg/scheduler/framework/plugins/volumebinding/OWNERS': True,
        'pkg/scheduler/framework/autoscaler_contract/OWNERS': True,
        'pkg/scheduler/apis/config/OWNERS': True,
        'pkg/routes/OWNERS': True,
        'pkg/printers/OWNERS': True,
        'pkg/registry/authorization/OWNERS': True,
        'pkg/registry/OWNERS': True,
        'pkg/registry/resource/OWNERS': True,
        'pkg/registry/admissionregistration/OWNERS': True,
        'pkg/registry/rbac/OWNERS': True,
        'pkg/registry/autoscaling/OWNERS': True,
        'pkg/registry/batch/OWNERS': True,
        'pkg/registry/apps/OWNERS': True,
        'pkg/registry/networking/OWNERS': True,
        'pkg/registry/policy/OWNERS': True,
        'pkg/registry/certificates/OWNERS': True,
        'pkg/registry/registrytest/OWNERS': True,
        'pkg/registry/storage/OWNERS': True,
        'pkg/registry/scheduling/OWNERS': True,
        'pkg/registry/core/OWNERS': True,
        'pkg/registry/core/service/OWNERS': True,
        'pkg/registry/core/podtemplate/OWNERS': True,
        'pkg/registry/core/pod/OWNERS': True,
        'pkg/registry/core/persistentvolumeclaim/OWNERS': True,
        'pkg/registry/core/serviceaccount/OWNERS': True,
        'pkg/registry/core/replicationcontroller/OWNERS': True,
        'pkg/registry/core/node/OWNERS': True,
        'pkg/registry/core/rangeallocation/OWNERS': True,
        'pkg/registry/core/persistentvolume/OWNERS': True,
        'pkg/registry/events/OWNERS': True,
        'pkg/registry/node/OWNERS': True,
        'pkg/registry/authentication/OWNERS': True,
        'pkg/apis/authorization/OWNERS': True,
        'pkg/apis/OWNERS': True,
        'pkg/apis/extensions/OWNERS': True,
        'pkg/apis/resource/OWNERS': True,
        'pkg/apis/rbac/OWNERS': True,
        'pkg/apis/autoscaling/OWNERS': True,
        'pkg/apis/batch/OWNERS': True,
        'pkg/apis/apps/OWNERS': True,
        'pkg/apis/networking/OWNERS': True,
        'pkg/apis/policy/OWNERS': True,
        'pkg/apis/certificates/OWNERS': True,
        'pkg/apis/imagepolicy/OWNERS': True,
        'pkg/apis/storage/OWNERS': True,
        'pkg/apis/core/v1/OWNERS': True,
        'pkg/apis/core/OWNERS': True,
        'pkg/apis/core/validation/OWNERS': True,
        'pkg/apis/core/install/OWNERS': True,
        'pkg/apis/abac/OWNERS': True,
        'pkg/apis/authentication/OWNERS': True,
        'pkg/probe/OWNERS': True,
        'pkg/client/OWNERS': True,
        'pkg/client/testdata/OWNERS': True,

    }

    OWNER = 'kubernetes'
    REPO = 'kubernetes'
    ORIGIN = 'https://github.com/kubernetes/kubernetes'

    def get_developer_info(self, filepath, code_owner_content):
        owners_yaml = None
        try:
            owners_yaml = yaml.safe_load(code_owner_content)
        except (yaml.scanner.ScannerError, yaml.parser.ParserError) as e:
            logger.error(f'Failed to parse {filepath}, content:\n{code_owner_content}\nError: {e}')
            return []

        if not owners_yaml:
            logger.warning(f'Empty yaml object at {filepath}, content:\n{code_owner_content}')
            return []
        developer_infos = []
        # For dict like {'foo': None}, the value of get will just return None(the second parameter only works when key
        # is not in the dict). So we need to make up an empty list by 'or []'
        labels = owners_yaml.get('labels', []) or []
        for approver_login in owners_yaml.get('approvers', []) or []:
            developer_infos.append((approver_login, 'approver', labels))
        for reviewer_login in owners_yaml.get('reviewers', []) or []:
            developer_infos.append((reviewer_login, 'reviewer', labels))
        for emeritus_approver_login in owners_yaml.get('emeritus_approvers', []) or []:
            developer_infos.append((emeritus_approver_login, 'emeritus_approver', labels))
        return developer_infos

    def apply_row_info(self, row, filepath, developer_tup):
        login, owner_type, labels = developer_tup
        row['module'] = filepath.replace('/OWNERS', '')
        row['name'] = ''
        row['email'] = ''
        row['misc'] = json.dumps({'labels': labels, 'owner_type': owner_type})
        row['github_login'] = login
