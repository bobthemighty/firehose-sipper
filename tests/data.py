from mimesis.enums import Gender
from mimesis.locales import Locale
from mimesis.schema import Field, Schema

_ = Field(locale=Locale.EN)
schema = Schema(schema=lambda: {
    "pk": _("increment"),
    "uid": _("uuid"),
    "name": _("text.word"),
    "version": _("version", pre_release=True),
    "timestamp": _("timestamp", posix=False),
    "owner": {
        "email": _("person.email", domains=["test.com"], key=str.lower),
        "token": _("token_hex"),
        "creator": _("full_name", gender=Gender.FEMALE),
    },
})

