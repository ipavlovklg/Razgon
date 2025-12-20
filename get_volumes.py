from win32com.client import GetObject

class VolumeInfo:
    """Information about a volume."""
    drive_letter: str
    device_id: str
    label: str
    fs: str

    def __init__(self, drive_letter: str, device_id: str, label: str, fs: str) -> None:
        self.drive_letter = drive_letter
        self.device_id = device_id
        self.label = label
        self.fs = fs
    
    pass


def get_volumes() -> list[VolumeInfo]:
    """Obtains the attahced volumes with their IDs, letters, labels, fs."""
    result = []
    wmi = GetObject("winmgmts:")
    query = "SELECT DriveLetter, DeviceID, Label, FileSystem FROM Win32_Volume WHERE DriveLetter IS NOT NULL"
    volumes = wmi.ExecQuery(query)

    for vol in volumes:
        device_id = vol.DeviceID or ""
        if device_id.startswith("\\\\?\\Volume{") and device_id.endswith("\\"):
            device_id = device_id[4:-1]

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
        print(f"Буква: {volumeInfo.drive_letter}")
        print(f"Volume GUID: {volumeInfo.device_id}")
        print(f"Метка: {volumeInfo.label}")
        print(f"Файловая система: {volumeInfo.fs}")
        print("-" * 50)