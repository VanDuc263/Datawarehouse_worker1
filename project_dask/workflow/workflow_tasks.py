from utils.mail_utils import send_error_mail

import traceback

def run_extract():
    try:
        from extract.extract import main
        
        raw_path = main()
        print(raw_path)
        return raw_path
    except Exception as e:
        tb = traceback.format_exc()
        print("❌ Lỗi run_extract:\n", tb)  # In ra console Worker
        send_error_mail(
            subject="[ETL ERROR] Extract Failed",
            message=tb,
            to_email="22130050@st.hcmuaf.edu.vn"
        )
        raise


def run_transform():
    try:
        from transform.transform_script import main
        return main()
    except Exception as e:
        send_error_mail(
            subject="[ETL ERROR] Transform Failed",
            message=str(e),
            to_email="22130050@st.hcmuaf.edu.vn"
        )
        raise e

