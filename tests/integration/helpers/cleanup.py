import logging
import shutil
from datetime import UTC, datetime

import httpx
from sqlalchemy import or_
from sqlmodel import Session

from app import runtime
from app.repositories.grafana_repository import GrafanaRepository
from app.domain.peer_model import Peer
from app.domain.repo_model import Repo
from app.domain.user_model import User
from app.dto.enum import PeerTrustStatus
from app.repositories.git_repo_repository import GitRepoRepository
from app.repositories.peer_repository import PeerRepository
from app.services.peer_service import PeerService
from tests.integration.helpers.keys import ADMIN_USER_KEY, REGULAR_USER_KEY
from tests.integration.helpers.names import TEST_HASH
from tests.integration.helpers.private_repos import all_known_repo_urls


def ensure_own_peer(database: Session) -> None:
    own_url = PeerService.get_own_url()
    repository = PeerRepository(db=database)
    existing = database.query(Peer).filter(Peer.url == own_url).first()
    if existing:
        return

    repository.create(
        Peer(
            url=own_url,
            trust_status=PeerTrustStatus.TRUST.value,
            create_datetime=datetime.now(UTC),
        )
    )


def _drop_test_repos(database: Session) -> None:
    urls = all_known_repo_urls()
    if not urls:
        return

    git_repo_repository = GitRepoRepository()
    repos = database.query(Repo).where(Repo.repository_url.in_(urls)).all()
    for repo in repos:
        git_repo_repository.delete_repo(repo)

    database.query(Repo).where(Repo.repository_url.in_(urls)).delete()


def drop_grafana_orgs(users: list[User]) -> None:
    """Grafana orgs live outside the DB and would pile up between runs."""
    if not runtime.get().pu_ff_grafana_integration_enable:
        return

    repository = GrafanaRepository(None)
    for user in users:
        org_id = user.grafana_org_id
        # Org 1 is the main Grafana org and must never be removed
        if not org_id or str(org_id) == "1":
            continue
        try:
            repository.delete_org(str(org_id))
        except httpx.HTTPError:
            logging.warning("Grafana org %s was not removed", org_id)


def clear_integration_data(database: Session) -> None:
    shutil.rmtree("tmp/test_units", ignore_errors=True)
    shutil.rmtree("tmp/test_units_tar_tgz", ignore_errors=True)
    _drop_test_repos(database)

    # OperationTask is deleted by cascade together with test Users
    own_url = PeerService.get_own_url()
    database.query(Peer).where(
        Peer.url.ilike(f"%{TEST_HASH}%"),
        Peer.url != own_url,
    ).delete()

    # Login is written after insert. A failed run can leave the stable test
    # key with an empty login, which the login filter does not see.
    test_users = database.query(User).where(
        or_(
            User.login.ilike(f"%{TEST_HASH}%"),
            User.hash_public_key.in_(
                [
                    REGULAR_USER_KEY.hash_public_key,
                    ADMIN_USER_KEY.hash_public_key,
                ]
            ),
        )
    )
    drop_grafana_orgs(test_users.all())
    test_users.delete()
    database.commit()
    ensure_own_peer(database)
