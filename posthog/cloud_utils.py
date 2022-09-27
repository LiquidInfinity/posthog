from django.conf import settings


def is_cloud():
    # TODO: Possibly cache this for a time?
    try:
        from ee.models import License

        license = License.objects.first_valid()
        return license.plan == "cloud" if license else settings.MULTI_TENANCY
    except ImportError:
        return False