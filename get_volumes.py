from win32com.client import GetObject

class VolumeInfo:
    """Information about a volume."""
    letter: str
    volume_guid: str
    label: str
    filesystem: str

    def __init__(self, letter: str, volume_guid: str, label: str, filesystem: str) -> None:
        self.letter = letter
        self.volume_guid = volume_guid
        self.label = label
        self.filesystem = filesystem
    
    pass


def get_volumes() -> list[VolumeInfo]:
    """Obtains the attahced volumes with their IDs, letters, labels, fs."""
    result = []
    wmi = GetObject("winmgmts:")
    query = "SELECT DriveLetter, DeviceID, Label, FileSystem FROM Win32_Volume WHERE DriveLetter IS NOT NULL"
    volumes = wmi.ExecQuery(query)

    for vol in volumes:
        device_id = vol.DeviceID or ""
        if device_id.startswith("\\\\?\\Volume{") and device_id.endswith("}\\"):
            device_id = device_id[10:-1]

        item = VolumeInfo(
            vol.DriveLetter, 
            device_id, 
            (vol.Label or "").strip(), 
            (vol.FileSystem or "").strip()
        )
        result.append(item)

    return result


if __name__ == "__main__":
    items = get_volumes()
    for volumeInfo in items:
        print(f"Буква: {volumeInfo.letter}")
        print(f"Volume GUID: {volumeInfo.volume_guid}")
        print(f"Метка: {volumeInfo.label}")
        print(f"Файловая система: {volumeInfo.filesystem}")
        print("-" * 50)