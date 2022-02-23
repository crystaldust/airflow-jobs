import itertools
from random import shuffle as shuffle_list


class _proxy_iter:
    def __init__(self, tokens):
        self.tokens_iter = itertools.cycle(tokens)

    def next(self):
        pass


class _cycle_iter(_proxy_iter):
    def __init__(self, tokens, proxies):
        super().__init__(tokens)
        self.proxies_iter = itertools.cycle(proxies)

    def next(self):
        return next(self.tokens_iter), next(self.proxies_iter)


class _fixed_map_iter(_proxy_iter):
    def __init__(self, tokens, proxies):
        super().__init__(tokens)

        self.mapping = {}
        num_proxies = len(proxies)
        for index, token in enumerate(tokens):
            mapped_index = index % num_proxies
            self.mapping[token] = proxies[mapped_index]

    def next(self):
        token = next(self.tokens_iter)
        proxy = self.mapping[token]
        return token, proxy


class GithubTokenProxyAccommodator:
    """An helper class to accommodate token and proxy"""
    POLICY_CYCLE_ITERATION = 'cycle_iteration'
    POLICY_FIXED_MAP = 'fixed_map'

    def __init__(self, tokens, proxies, policy='cycle_iteration', shuffle=True):
        """
        :param tokens: List of github tokens
        :param proxies: List of proxies dict with form {"scheme":"", "ip": "", "port":123, "user": "", "password": ""}
        :param policy: Accommodation policy, currently support POLICY_CYCLE_ITERATION and POLICY_FIXED_MAP. In POLICY_CYCLE_ITERATION'
        policy, both tokens and proxies are iterated in a cycle. In POLICY_FIXED_MAP policy, a token pairs with a fixed proxy,
        all tokens are paired.
        :param shuffle: Whether tokens and proxies should be shuffled before building accommodator.
        This allows some randomity
        """
        if shuffle:
            shuffle_list(tokens)
            shuffle_list(proxies)
        self.tokens = tokens
        self.proxies = proxies
        self.policy = policy
        self._accommodate()

    def _accommodate(self):
        if self.policy == GithubTokenProxyAccommodator.POLICY_CYCLE_ITERATION:
            self._iter = _cycle_iter(self.tokens, self.proxies)
        elif self.policy == GithubTokenProxyAccommodator.POLICY_FIXED_MAP:
            self._iter = _fixed_map_iter(self.tokens, self.proxies)

    def next(self):
        return self._iter.next()
