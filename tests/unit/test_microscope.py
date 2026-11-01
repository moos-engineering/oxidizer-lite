from oxidizer import Microscope
from oxidizer.utils.microscope import Microscope as MicroscopePlaceholder


def test_microscope_is_an_importable_placeholder():
    assert Microscope is MicroscopePlaceholder
    assert vars(Microscope()) == {}


def test_microscope_can_be_subclassed():
    class CustomMicroscope(Microscope):
        pass

    assert isinstance(CustomMicroscope(), Microscope)
