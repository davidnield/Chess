"""Windows process lifetime and unbuffered archive checksums."""
import ctypes,hashlib,os
from ctypes import wintypes as w
from pathlib import Path

def kill_children_on_exit():
    """Assign this coordinator to a job; descendants die when its sole job handle closes.
    Return the handle and keep it alive until process exit. Do not explicitly close it.
    """
    if os.name!='nt':raise RuntimeError('archive coordinator currently requires Windows')
    k=ctypes.WinDLL('kernel32',use_last_error=True)
    class Basic(ctypes.Structure):
        _fields_=[('p',ctypes.c_longlong),('j',ctypes.c_longlong),('flags',w.DWORD),('min',ctypes.c_size_t),('max',ctypes.c_size_t),('active',w.DWORD),('affinity',ctypes.c_size_t),('priority',w.DWORD),('schedule',w.DWORD)]
    class IO(ctypes.Structure):_fields_=[(n,ctypes.c_ulonglong) for n in ('r','w','o','rb','wb','ob')]
    class Ext(ctypes.Structure):_fields_=[('basic',Basic),('io',IO),('pm',ctypes.c_size_t),('jm',ctypes.c_size_t),('pp',ctypes.c_size_t),('jp',ctypes.c_size_t)]
    k.CreateJobObjectW.argtypes=[ctypes.c_void_p,w.LPCWSTR];k.CreateJobObjectW.restype=w.HANDLE
    k.SetInformationJobObject.argtypes=[w.HANDLE,ctypes.c_int,ctypes.c_void_p,w.DWORD]
    k.AssignProcessToJobObject.argtypes=[w.HANDLE,w.HANDLE];k.GetCurrentProcess.restype=w.HANDLE
    handle=k.CreateJobObjectW(None,None);limits=Ext();limits.basic.flags=0x2000
    if not handle or not k.SetInformationJobObject(handle,9,ctypes.byref(limits),ctypes.sizeof(limits)) or not k.AssignProcessToJobObject(handle,k.GetCurrentProcess()):raise ctypes.WinError(ctypes.get_last_error())
    return handle

def unbuffered_sha(path, *, copy_to=None):
    """Read real file bytes without Windows' file cache, including a non-sector-aligned EOF. Optionally stage the exact bytes with exclusive create."""
    path=Path(path);before=path.stat();size=before.st_size
    k=ctypes.WinDLL('kernel32',use_last_error=True)
    k.CreateFileW.argtypes=[w.LPCWSTR,w.DWORD,w.DWORD,ctypes.c_void_p,w.DWORD,w.DWORD,w.HANDLE];k.CreateFileW.restype=w.HANDLE
    k.VirtualAlloc.argtypes=[ctypes.c_void_p,ctypes.c_size_t,w.DWORD,w.DWORD];k.VirtualAlloc.restype=ctypes.c_void_p
    k.VirtualFree.argtypes=[ctypes.c_void_p,ctypes.c_size_t,w.DWORD]
    k.ReadFile.argtypes=[w.HANDLE,ctypes.c_void_p,w.DWORD,ctypes.POINTER(w.DWORD),ctypes.c_void_p];k.CloseHandle.argtypes=[w.HANDLE]
    block=8<<20;buf=k.VirtualAlloc(None,block,0x3000,4)
    if not buf:raise ctypes.WinError(ctypes.get_last_error())
    handle=k.CreateFileW(str(path),0x80000000,1,None,3,0x20000000|0x08000000,None)
    staged=None
    try:
        if handle==ctypes.c_void_p(-1).value:raise ctypes.WinError(ctypes.get_last_error())
        if copy_to is not None:staged=Path(copy_to).open('xb')
        view=memoryview((ctypes.c_ubyte*block).from_address(buf)).cast('B');h=hashlib.sha256();total=0
        while total<size:
            got=w.DWORD()
            if not k.ReadFile(handle,buf,block,ctypes.byref(got),None):raise ctypes.WinError(ctypes.get_last_error())
            expected=min(block,size-total)
            if not expected<=got.value<=block:raise ValueError('short archive read')
            h.update(view[:expected]);total+=expected
            if staged is not None:staged.write(view[:expected])
        if staged is not None:staged.flush();os.fsync(staged.fileno())
        after=path.stat()
        if (before.st_size,before.st_mtime_ns,before.st_ino)!=(after.st_size,after.st_mtime_ns,after.st_ino):raise ValueError('archive changed while hashing')
        return h.hexdigest()
    finally:
        if staged is not None:staged.close()
        if handle!=ctypes.c_void_p(-1).value:k.CloseHandle(handle)
        k.VirtualFree(buf,0,0x8000)
