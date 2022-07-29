from smb.SMBConnection import SMBConnection

conn = SMBConnection(username="dbs01", password="S@LeS*41011", my_name="Alexander Brezhnev", remote_name="samba", use_ntlm_v2=True)

smb_file = '/10_report/F_result.csv'

if conn.connect("10.88.22.128", 445):
    print(conn.listShares())
    with open('/media/sasha/DATA/F_result.csv', 'rb') as f:
        conn.storeFile('dbs', smb_file, f)

# \\10.88.22.128\dbs\10_report
#
# conn = SMBConnection("dbs01", "S@LeS*41011", "vlad", "samba", use_ntlm_v2=True)
# if conn.connect("10.88.22.128", 445):
#     with open('/home/vlad/Загрузки/reconnect.bat', 'r', encoding='utf-8') as f:
#         conn.storeFile('dbs', '/testv/reconnecct.bat', f)
# , encoding='utf-8'
