from src.auth.infrastructure.user_repository import DbFileUserRepository


def test_get_by_email_exact_match():
    repo = DbFileUserRepository()
    user = repo.get_by_email("p23dsc103@nmc.ac.in")
    assert user is not None
    assert user.tenant_id == "nmc"
    assert user.role == "student"


def test_get_by_email_case_insensitive():
    repo = DbFileUserRepository()
    user = repo.get_by_email("P23DSC103@nmc.ac.in")
    assert user is not None
    assert user.email == "p23dsc103@nmc.ac.in"


def test_get_by_email_missing():
    repo = DbFileUserRepository()
    assert repo.get_by_email("unknown@example.com") is None
