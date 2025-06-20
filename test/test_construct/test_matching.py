import gzip
import tempfile
from typing import List

import pytest

from tgrag.utils.matching import extract_graph_domains


@pytest.fixture
def sample_domain() -> List[str]:
    return ['92047 com.xbox']


@pytest.fixture
def sample_domain_multilingual() -> List[str]:
    return [
        '622932 com.stylig',
        '622933 com.stylig.cz',
        '622934 com.stylig.de',
    ]


@pytest.fixture
def sample_domain_lingual_prefix() -> List[str]:
    return ['96168 cz.zvb.forum', '96169 cz.zvb']


@pytest.fixture
def sample_different_domains() -> List[str]:
    return ['123 com.reuters', '124 com.thomsonreuters']


@pytest.fixture
def sample_different_TLD() -> List[str]:
    # TLD = Top-Level Domain
    return ['123 com.whitehouse', '124 gov.whitehouse']


def test_extract_graph_domains_basic(sample_domain: List[str]) -> None:
    expected_value = 'xbox.com'

    with tempfile.NamedTemporaryFile(suffix='.gz') as tmp:
        with gzip.open(tmp.name, 'wt', encoding='utf-8') as f:
            for line in sample_domain:
                f.write(line + '\n')

        result = extract_graph_domains(tmp.name)

    assert all(
        val == expected_value for val in result['match_domain']
    ), 'Expected basic domain extraction to match domain name (xbox.com).'


def test_extract_graph_domains_multilingual(
    sample_domain_multilingual: List[str],
) -> None:
    expected_value = 'stylig.com'

    with tempfile.NamedTemporaryFile(suffix='.gz') as tmp:
        with gzip.open(tmp.name, 'wt', encoding='utf-8') as f:
            for line in sample_domain_multilingual:
                f.write(line + '\n')

        result = extract_graph_domains(tmp.name)

    assert all(
        val == expected_value for val in result['match_domain']
    ), 'Expected multilingual domains to yield the same base domain name (stylig.com).'


def test_extract_graph_domains_different_prefix(
    sample_domain_lingual_prefix: List[str],
) -> None:
    expected_value = 'zvb.cz'

    with tempfile.NamedTemporaryFile(suffix='.gz') as tmp:
        with gzip.open(tmp.name, 'wt', encoding='utf-8') as f:
            for line in sample_domain_lingual_prefix:
                f.write(line + '\n')

        result = extract_graph_domains(tmp.name)

    assert all(
        val == expected_value for val in result['match_domain']
    ), 'Expected different prefix to still yield valid domain name.'


def test_extract_different_domains(sample_different_domains: List[str]) -> None:
    with tempfile.NamedTemporaryFile(suffix='.gz') as tmp:
        with gzip.open(tmp.name, 'wt', encoding='utf-8') as f:
            for line in sample_different_domains:
                f.write(line + '\n')

        result = extract_graph_domains(tmp.name)

    assert (
        result.iloc[0]['match_domain'] != result.iloc[1]['match_domain']
    ), 'Expected different domains to be extracted'


def test_extract_different_TLD(sample_different_TLD: List[str]) -> None:
    with tempfile.NamedTemporaryFile(suffix='.gz') as tmp:
        with gzip.open(tmp.name, 'wt', encoding='utf-8') as f:
            for line in sample_different_TLD:
                f.write(line + '\n')

        result = extract_graph_domains(tmp.name)

    assert (
        result.iloc[0]['match_domain'] != result.iloc[1]['match_domain']
    ), 'Expected different top-level domains to be extracted as different domains.'
