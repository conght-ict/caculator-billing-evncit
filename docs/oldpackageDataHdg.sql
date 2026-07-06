   PROCEDURE SP_GETCUSTOMERDATA (p_MA_DVIQLY           IN     VARCHAR2,
                                  p_MA_SO_GCS           IN     VARCHAR2,
                                  p_KY                  IN     NUMBER,
                                  p_THANG               IN     NUMBER,
                                  p_NAM                 IN     NUMBER,
                                  p_NGAY_GHI            IN     VARCHAR2,
                                  p_HDG_DDO_SOGCS          OUT rst,
                                  p_HDG_QHE_DDO            OUT rst,
                                  p_HDG_DIEM_DO            OUT rst,
                                  p_HDG_BBAN_APGIA         OUT rst,
                                  p_HDG_KHACH_HANG         OUT rst,
                                  p_HDG_PTHUC_TTOAN        OUT rst,
                                  p_HDG_VITRI_DDO          OUT rst,
                                  p_HDG_DIEM_DO_GT         OUT rst,
                                  p_HDG_DDO_SOGCS_GT       OUT rst,
                                  p_HDG_BBAN_APGIA_GT      OUT rst,
                                  p_HDG_VITRI_DDO_GT       OUT rst,
                                  p_HDG_KHACH_HANG_TT      OUT rst,
                                  p_HDG_QHE_DDO_TP         OUT rst,
                                  p_HDG_QHE_DDO_BQ         OUT rst,
                                  p_HDG_QHE_DDO_GT         OUT rst,
                                  p_HDG_DDO_GTRU         OUT rst,
                                  p_HDG_DDO_TDOIGCS     OUT rst)
    IS
        p_NGAY_CKY   DATE := TO_DATE (p_NGAY_GHI, 'dd/MM/yyyy');
        p_ERROR NVARCHAR2(1000);
    BEGIN
        -- cap nhat hoa don giam tru covid 202012:
        --SP_CREATE_HDGTRU_COVID202012(p_MA_DVIQLY,p_MA_SO_GCS,p_KY,p_THANG,p_NAM,p_NGAY_GHI,p_ERROR);
        -- HDG_DDO_SOGCS
        OPEN p_HDG_DDO_SOGCS FOR
            SELECT DDO.MA_DVIQLY,
                   DDO.ID_QHE,
                   DDO.MA_SOGCS,
                   DDO.MA_DDO,
                   DDO.MA_KHANG,
                   TO_CHAR (DDO.NGAY_HLUC, 'dd/MM/yyyy') AS NGAY_HLUC,
                   DDO.MA_KVUC,
                   DDO.STT              
              FROM HDG_DDO_SOGCS DDO
             WHERE     MA_DVIQLY = p_MA_DVIQLY
                   AND MA_SOGCS = p_MA_SO_GCS
                   AND NGAY_HLUC <= p_NGAY_CKY                                   
               AND (NGAY_HHLUC IS NULL OR NGAY_HHLUC > p_NGAY_CKY)
               AND KY + 12 * THANG + 12 * 12 * NAM <=
                                       p_KY + 12 * p_THANG + 12 * 12 * p_NAM
                   AND NOT EXISTS
                           (SELECT 1
                              FROM HDG_QHE_DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND NAM = p_NAM
                                   AND THANG = p_THANG
                                   AND KY = p_KY
                                   AND MA_SOGCS_CHINH <> MA_SOGCS_PHU
                                   AND MA_SOGCS_PHU = p_MA_SO_GCS
                                   AND MA_DDO_PHU = DDO.MA_DDO
                                   AND LOAI_QHE = '40');

        -- HDG_QHE_DDO
        OPEN p_HDG_QHE_DDO FOR
            SELECT H.MA_DVIQLY,
                   H.ID_QHE,
                   H.MA_KHANG_CHINH,
                   H.MA_KHANG_PHU,
                   H.MA_DDO_CHINH,
                   H.MA_DDO_PHU,
                   H.MA_SOGCS_CHINH,
                   H.MA_SOGCS_PHU,
                   H.LOAI_QHE,
                   H.TT_UUTIEN,
                   H.THANG,
                   H.NAM,
                   H.KY,
                   H.KY_P,
                   H.KY_HOADON
              --           ,
              --           H.NGAY_TAO, H.NGUOI_TAO, H.NGAY_SUA,
              --           H.NGUOI_SUA, H.MA_CNANG
              FROM HDG_QHE_DDO H
             WHERE     MA_DVIQLY = p_MA_DVIQLY
                   AND NAM = p_NAM
                   AND THANG = p_THANG
                   AND KY = p_KY
                   AND MA_SOGCS_CHINH = p_MA_SO_GCS
            --Lay them cac quan he tru phu c?a diem do phu ghep tong
            UNION
            (SELECT H.MA_DVIQLY,
                    H.ID_QHE,
                    H.MA_KHANG_CHINH,
                    H.MA_KHANG_PHU,
                    H.MA_DDO_CHINH,
                    H.MA_DDO_PHU,
                    H.MA_SOGCS_CHINH,
                    H.MA_SOGCS_PHU,
                    H.LOAI_QHE,
                    H.TT_UUTIEN,
                    H.THANG,
                    H.NAM,
                    H.KY,
                    H.KY_P,
                    H.KY_HOADON
               --                     ,
               --                     H.NGAY_TAO, H.NGUOI_TAO, H.NGAY_SUA,
               --                     H.NGUOI_SUA, H.MA_CNANG
               FROM HDG_QHE_DDO H
              WHERE     MA_DVIQLY = p_MA_DVIQLY
                    AND NAM = p_NAM
                    AND THANG = p_THANG
                    AND KY = p_KY
                    AND LOAI_QHE <> '40'
                    AND LOAI_QHE <> '32'
                    AND MA_DDO_CHINH IN
                            (SELECT MA_DDO_PHU
                               FROM HDG_QHE_DDO
                              WHERE     MA_DVIQLY = p_MA_DVIQLY
                                    AND NAM = p_NAM
                                    AND THANG = p_THANG
                                    AND KY = p_KY
                                    AND LOAI_QHE = '40'
                                    AND MA_SOGCS_CHINH = p_MA_SO_GCS
                                    AND MA_SOGCS_CHINH <> MA_SOGCS_PHU));

        -- HDG_DIEM_DO
        OPEN p_HDG_DIEM_DO FOR
            SELECT H.MA_DVIQLY,
                   H.MA_DDO,
                   H.MA_KHANG,
                   H.MA_CAPDA,
                   H.DIA_CHI,
                   H.ID_DIA_CHINH,
                   H.KIMUA_CSPK,
                   H.CSUAT,
                   H.SO_PHA,
                   H.SOHUU_LUOI,
                   H.LOAI_DDO,
                   H.SO_HO,
                   1                                   LAN_CAPNHAT,
                   H.THAO_TACDL,
                   TO_CHAR (H.NGAY_HLUC, 'dd/MM/yyyy') AS NGAY_HLUC,
                   1                                   TRANG_THAI,
                   --           H.NGAY_TAO, H.NGUOI_TAO, H.NGAY_SUA,
                   --           H.NGUOI_SUA, H.MA_CNANG,
--                    (SELECT NVL (TO_CHAR (MAX (NGAY_HLUC), 'dd/MM/yyyy'),
--                                 '01/01/2000')
--                       FROM HDG_TDOITTIN
--                      WHERE     ma_dviqly = H.ma_dviqly
--                            AND ID_KT_CU = H.ID_DDO
--                            AND TEN_COT = 'KIMUA_CSPK'
--                            AND TEN_BANG = 'HDG_DIEM_DO')
                   '01/01/1900'
                       AS NGAY_DOI_CSPK
              FROM HDG_DIEM_DO H
             WHERE     MA_DVIQLY = p_MA_DVIQLY
                   AND MA_DDO IN
                           (SELECT DDO.MA_DDO
                              FROM HDG_DDO_SOGCS DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND MA_SOGCS = p_MA_SO_GCS
                                   AND NGAY_HLUC <= p_NGAY_CKY                                   
                                    AND (NGAY_HHLUC IS NULL OR NGAY_HHLUC > p_NGAY_CKY)
                                    AND KY + 12 * THANG + 12 * 12 * NAM <= p_KY + 12 * p_THANG + 12 * 12 * p_NAM
                                   AND NOT EXISTS
                                           (SELECT 1
                                              FROM HDG_QHE_DDO
                                             WHERE     MA_DVIQLY =
                                                           p_MA_DVIQLY
                                                   AND NAM = p_NAM
                                                   AND THANG = p_THANG
                                                   AND KY = p_KY
                                                   AND MA_SOGCS_CHINH <>
                                                           MA_SOGCS_PHU
                                                   AND MA_SOGCS_PHU =
                                                           p_MA_SO_GCS
                                                   AND MA_DDO_PHU =
                                                           DDO.MA_DDO
                                                   AND LOAI_QHE = '40'))
                   AND H.NGAY_HLUC <= p_NGAY_CKY                                   
               AND (H.NGAY_HHLUC IS NULL OR H.NGAY_HHLUC > p_NGAY_CKY);

        -- HDG_BBAN_APGIA
        OPEN p_HDG_BBAN_APGIA FOR
            SELECT MA_DVIQLY,
                   ID_BBANAGIA,
                   MA_BBANAGIA,
                   MA_DDO,
                   TO_CHAR (NGAY_HLUC, 'dd/MM/yyyy')  AS NGAY_HLUC,
                   SO_THUTU,
                   DINH_MUC,
                   LOAI_DMUC,
                   LOAI_BCS,
                   TGIAN_BDIEN,
                   MA_NHOMNN,
                   MA_NGIA,
                   MA_NN,
                   TRANG_THAI,
                   SO_HO,
                   MA_CAPDAP--   ,
                            --           DIEN_GIAI, NGAY_TAO,
                            --           NGUOI_TAO, NGAY_SUA, NGUOI_SUA,
                            --           MA_CNANG
                            ,
                   DECODE (THAO_TACDL, 'TDOI_SOHO', 1, 0) AS IS_SOHO
              --,(select COUNT(*) from HDG_DIEM_DO where MA_DVIQLY=H.MA_DVIQLY and MA_DDO=H.MA_DDO and NGAY_HLUC = H.NGAY_HLUC and THAO_TACDL='SOHO') as IS_SOHO
              FROM HDG_BBAN_APGIA H
             WHERE     MA_DVIQLY = p_MA_DVIQLY AND TRANG_THAI = 1
                   AND MA_DDO IN
                           (SELECT DDO.MA_DDO
                              FROM HDG_DDO_SOGCS DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND MA_SOGCS = p_MA_SO_GCS
                                   AND NGAY_HLUC <= p_NGAY_CKY                                   
                                    AND (NGAY_HHLUC IS NULL OR NGAY_HHLUC > p_NGAY_CKY)
                                    AND KY + 12 * THANG + 12 * 12 * NAM <= p_KY + 12 * p_THANG + 12 * 12 * p_NAM
                                   AND NOT EXISTS
                                           (SELECT 1
                                              FROM HDG_QHE_DDO
                                             WHERE     MA_DVIQLY =
                                                           p_MA_DVIQLY
                                                   AND NAM = p_NAM
                                                   AND THANG = p_THANG
                                                   AND KY = p_KY
                                                   AND MA_SOGCS_CHINH <>
                                                           MA_SOGCS_PHU
                                                   AND MA_SOGCS_PHU =
                                                           p_MA_SO_GCS
                                                   AND MA_DDO_PHU =
                                                           DDO.MA_DDO
                                                   AND LOAI_QHE = '40')
                            --Them cac diem do tru phu cua diem do phu ghep tong-khong co quan he cosfi binh quan
                            UNION
                            (SELECT MA_DDO_PHU AS MA_DDO
                               FROM HDG_QHE_DDO
                              WHERE     MA_DVIQLY = p_MA_DVIQLY
                                    AND NAM = p_NAM
                                    AND THANG = p_THANG
                                    AND KY = p_KY
                                    AND LOAI_QHE <> '40'
                                    AND LOAI_QHE <> '32'
                                    AND MA_DDO_CHINH IN
                                            (SELECT MA_DDO_PHU
                                               FROM HDG_QHE_DDO
                                              WHERE     MA_DVIQLY =
                                                            p_MA_DVIQLY
                                                    AND NAM = p_NAM
                                                    AND THANG = p_THANG
                                                    AND KY = p_KY
                                                    AND LOAI_QHE = '40'
                                                    AND MA_SOGCS_CHINH =
                                                            p_MA_SO_GCS
                                                    AND MA_SOGCS_CHINH <>
                                                            MA_SOGCS_PHU)))
                   AND NGAY_HLUC <= p_NGAY_CKY;


        -- HDG_KHACH_HANG
        OPEN p_HDG_KHACH_HANG FOR
            SELECT H1.MA_DVIQLY,
                   H1.MA_KHANG,
                   H1.SO_NHA,
                   H1.DUONG_PHO,
                   H1.MASO_THUE,
                   H1.TLE_THUE,
                   H1.LOAI_KHANG,
                   H1.MANHOM_KHANG,
                   H1.MA_NN,
                   H1.TEN_KHANG,
                   TO_CHAR (H1.NGAY_HLUC, 'dd/MM/yyyy') AS NGAY_HLUC,
                   1                                   TRANG_THAI,
                   H1.THANG,
                   H1.NAM,
                   H1.TKHOAN_KHANG,
                   H1.MA_NHANG,
                   H1.MA_KHTT,
                   H1.MA_LOAIDN,
                   NVL(H2.DTHOAI_DVU, H1.DTHOAI) DTHOAI,
                   H1.FAX,
                   H1.EMAIL,
                   H1.WEBSITE,
                   H1.GIOI_TINH,
                   ' '                                 AS DCHI_TTOAN
                   from
                   (select *
              FROM HDG_KHACH_HANG K
             WHERE     K.MA_DVIQLY = p_MA_DVIQLY
                   AND K.MA_KHANG IN
                           (SELECT DDO.MA_KHANG
                              FROM HDG_DDO_SOGCS DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND MA_SOGCS = p_MA_SO_GCS
                                   AND NGAY_HLUC <= p_NGAY_CKY                                   
                                    AND (NGAY_HHLUC IS NULL OR NGAY_HHLUC > p_NGAY_CKY)
                                    AND KY + 12 * THANG + 12 * 12 * NAM <= p_KY + 12 * p_THANG + 12 * 12 * p_NAM
                                   AND NOT EXISTS
                                           (SELECT 1
                                              FROM HDG_QHE_DDO
                                             WHERE     MA_DVIQLY =
                                                           p_MA_DVIQLY
                                                   AND NAM = p_NAM
                                                   AND THANG = p_THANG
                                                   AND KY = p_KY
                                                   AND MA_SOGCS_CHINH <>
                                                           MA_SOGCS_PHU
                                                   AND MA_SOGCS_PHU =
                                                           p_MA_SO_GCS
                                                   AND MA_DDO_PHU =
                                                           DDO.MA_DDO
                                                   AND LOAI_QHE = '40'))
                   AND K.NGAY_HLUC <= p_NGAY_CKY                                   
                                    AND (K.NGAY_HHLUC IS NULL OR K.NGAY_HHLUC > p_NGAY_CKY)) H1
             left join 
             (select K.MA_KHANG,K.DTHOAI_DVU, ROW_NUMBER() OVER(PARTITION BY K.MA_KHANG ORDER BY K.MA_KHANG) as SEQ 
              FROM HDG_KHANG_LIENHE K
             WHERE     K.MA_DVIQLY = p_MA_DVIQLY
                   AND K.MA_KHANG IN
                           (SELECT DDO.MA_KHANG
                              FROM HDG_DDO_SOGCS DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND MA_SOGCS = p_MA_SO_GCS
                                   AND NGAY_HLUC <= p_NGAY_CKY                                   
                                    AND (NGAY_HHLUC IS NULL OR NGAY_HHLUC > p_NGAY_CKY)
                                    AND KY + 12 * THANG + 12 * 12 * NAM <= p_KY + 12 * p_THANG + 12 * 12 * p_NAM
                                   AND NOT EXISTS
                                           (SELECT 1
                                              FROM HDG_QHE_DDO
                                             WHERE     MA_DVIQLY =
                                                           p_MA_DVIQLY
                                                   AND NAM = p_NAM
                                                   AND THANG = p_THANG
                                                   AND KY = p_KY
                                                   AND MA_SOGCS_CHINH <>
                                                           MA_SOGCS_PHU
                                                   AND MA_SOGCS_PHU =
                                                           p_MA_SO_GCS
                                                   AND MA_DDO_PHU =
                                                           DDO.MA_DDO
                                                   AND LOAI_QHE = '40'))
                   AND K.NGAY_HLUC <= p_NGAY_CKY                                   
                   AND (K.NGAY_HHLUC IS NULL OR K.NGAY_HHLUC > p_NGAY_CKY)
                   and STTU_UTIEN=0
                   ) H2
                   on H2.MA_KHANG=H1.MA_KHANG
                   and H2.SEQ=1;

        OPEN p_HDG_KHACH_HANG_TT FOR
            SELECT K.MA_DVIQLY,
                   K.MA_KHANG,
                   K.SO_NHA,
                   K.DUONG_PHO,
                   K.TEN_KHANG
              FROM HDG_KHACH_HANG K
             WHERE     K.MA_DVIQLY = p_MA_DVIQLY
                   AND K.NGAY_HLUC <= p_NGAY_CKY                                   
                                    AND (K.NGAY_HHLUC IS NULL OR K.NGAY_HHLUC > p_NGAY_CKY)                          
                   AND EXISTS
                           (SELECT MA_KHTT
                              FROM HDG_KHACH_HANG
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                    AND MA_KHTT = K.MA_KHANG
                                   AND MA_KHANG IN
                                           (SELECT DDO.MA_KHANG
                                              FROM HDG_DDO_SOGCS DDO
                                             WHERE     MA_DVIQLY =
                                                           p_MA_DVIQLY
                                                   AND MA_SOGCS = p_MA_SO_GCS
                                                   AND NGAY_HLUC <= p_NGAY_CKY                                   
                                                    AND (NGAY_HHLUC IS NULL OR NGAY_HHLUC > p_NGAY_CKY)
                                                    AND KY + 12 * THANG + 12 * 12 * NAM <= p_KY + 12 * p_THANG + 12 * 12 * p_NAM
                                                   AND NOT EXISTS
                                                           (SELECT 1
                                                              FROM HDG_QHE_DDO
                                                             WHERE     MA_DVIQLY =
                                                                           p_MA_DVIQLY
                                                                   AND NAM =
                                                                           p_NAM
                                                                   AND THANG =
                                                                           p_THANG
                                                                   AND KY =
                                                                           p_KY
                                                                   AND MA_SOGCS_CHINH <>
                                                                           MA_SOGCS_PHU
                                                                   AND MA_SOGCS_PHU =
                                                                           p_MA_SO_GCS
                                                                   AND MA_DDO_PHU =
                                                                           DDO.MA_DDO
                                                                   AND LOAI_QHE =
                                                                           '40'))
                                   AND K.NGAY_HLUC <= p_NGAY_CKY                                   
                                    AND (K.NGAY_HHLUC IS NULL OR K.NGAY_HHLUC > p_NGAY_CKY) );

        -- HDG_PTHUC_TTOAN
        OPEN p_HDG_PTHUC_TTOAN FOR
            SELECT K.MA_DVIQLY,
                   K.ID_PTHUC_TTOAN,
                   K.MA_KHANG,
                   TO_CHAR (K.NGAY_HLUC, 'dd/MM/yyyy') AS NGAY_HLUC,
                   1                                   TRANG_THAI,
                   K.HTHUC_TTOAN,
                   K.PTHUC_TTOAN
              --           ,
              --           K.NGAY_TAO, K.NGUOI_TAO,
              --           K.NGAY_SUA, K.NGUOI_SUA, K.MA_CNANG
              FROM HDG_PTHUC_TTOAN K
             WHERE     K.MA_DVIQLY = p_MA_DVIQLY
                   AND K.MA_KHANG IN
                           (SELECT DDO.MA_KHANG
                              FROM HDG_DDO_SOGCS DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND MA_SOGCS = p_MA_SO_GCS
                                   AND NGAY_HLUC <= p_NGAY_CKY                                   
                                   AND (NGAY_HHLUC IS NULL OR NGAY_HHLUC > p_NGAY_CKY)
                                   AND KY + 12 * THANG + 12 * 12 * NAM <=
                                       p_KY + 12 * p_THANG + 12 * 12 * p_NAM
                                   AND NOT EXISTS
                                           (SELECT 1
                                              FROM HDG_QHE_DDO
                                             WHERE     MA_DVIQLY =
                                                           p_MA_DVIQLY
                                                   AND NAM = p_NAM
                                                   AND THANG = p_THANG
                                                   AND KY = p_KY
                                                   AND MA_SOGCS_CHINH <>
                                                           MA_SOGCS_PHU
                                                   AND MA_SOGCS_PHU =
                                                           p_MA_SO_GCS
                                                   AND MA_DDO_PHU =
                                                           DDO.MA_DDO
                                                   AND LOAI_QHE = '40'))
                   AND K.NGAY_HLUC <= p_NGAY_CKY                                   
               AND (K.NGAY_HHLUC IS NULL OR K.NGAY_HHLUC > p_NGAY_CKY)               ;

        -- HDG_VITRI_DDO
        OPEN p_HDG_VITRI_DDO FOR
            SELECT H.MA_DVIQLY,
                   H.ID_VITRI_DDO,
                   TO_CHAR (H.NGAY_HLUC, 'dd/MM/yyyy') AS NGAY_HLUC,
                   H.MA_DDO,
                   H.MA_TRAM,
                   H.MA_LO,
                   H.MA_TO,
                   H.PHA,
                   H.SO_COT,
                   H.SO_HOP
              --           , H.NGAY_TAO, H.NGUOI_TAO,
              --           H.NGAY_SUA, H.NGUOI_SUA, H.MA_CNANG
              FROM HDG_VITRI_DDO H
             WHERE     MA_DVIQLY = p_MA_DVIQLY
                   AND MA_DDO IN
                           (SELECT DDO.MA_DDO
                              FROM HDG_DDO_SOGCS DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND MA_SOGCS = p_MA_SO_GCS
                                   AND NGAY_HLUC <= p_NGAY_CKY                                   
                                   AND (NGAY_HHLUC IS NULL OR NGAY_HHLUC > p_NGAY_CKY)
                                   AND KY + 12 * THANG + 12 * 12 * NAM <=
                                       p_KY + 12 * p_THANG + 12 * 12 * p_NAM
                                   AND NOT EXISTS
                                           (SELECT 1
                                              FROM HDG_QHE_DDO
                                             WHERE     MA_DVIQLY =
                                                           p_MA_DVIQLY
                                                   AND NAM = p_NAM
                                                   AND THANG = p_THANG
                                                   AND KY = p_KY
                                                   AND MA_SOGCS_CHINH <>
                                                           MA_SOGCS_PHU
                                                   AND MA_SOGCS_PHU =
                                                           p_MA_SO_GCS
                                                   AND MA_DDO_PHU =
                                                           DDO.MA_DDO
                                                   AND LOAI_QHE = '40'))
                   AND H.NGAY_HLUC <= p_NGAY_CKY                                   
               AND (H.NGAY_HHLUC IS NULL OR H.NGAY_HHLUC > p_NGAY_CKY)               ;


        -- HDG_DIEM_DO_GT
        OPEN p_HDG_DIEM_DO_GT FOR
            SELECT H.MA_DVIQLY,
                   H.MA_DDO,
                   H.MA_KHANG,
                   H.MA_CAPDA,
                   H.DIA_CHI,
                   H.ID_DIA_CHINH,
                   H.KIMUA_CSPK,
                   H.CSUAT,
                   H.SO_PHA,
                   H.SOHUU_LUOI,
                   H.LOAI_DDO,
                   H.SO_HO,
                   1                                   LAN_CAPNHAT,
                   H.THAO_TACDL,
                   TO_CHAR (H.NGAY_HLUC, 'dd/MM/yyyy') AS NGAY_HLUC,
                   1                                   TRANG_THAI,
                   --           H.NGAY_TAO, H.NGUOI_TAO, H.NGAY_SUA,
                   --           H.NGUOI_SUA, H.MA_CNANG,
--                    (SELECT NVL (TO_CHAR (MAX (NGAY_HLUC), 'dd/MM/yyyy'),
--                                 '01/01/2000')
--                       FROM HDG_TDOITTIN
--                      WHERE     ma_dviqly = H.ma_dviqly
--                            AND ID_KT_CU = H.ID_DDO
--                            AND TEN_COT = 'KIMUA_CSPK'
--                            AND TEN_BANG = 'HDG_DIEM_DO')
                   '01/01/1900'
                       AS NGAY_DOI_CSPK
              FROM HDG_DIEM_DO H
             WHERE     MA_DVIQLY = p_MA_DVIQLY
                   AND MA_DDO IN
                           (SELECT MA_DDO_PHU
                              FROM HDG_QHE_DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND NAM = p_NAM
                                   AND THANG = p_THANG
                                   AND KY = p_KY
                                   AND MA_SOGCS_CHINH <> MA_SOGCS_PHU
                                   AND MA_SOGCS_CHINH = p_MA_SO_GCS
                                   AND LOAI_QHE = '40')
                   AND H.NGAY_HLUC <= p_NGAY_CKY                                   
               AND (H.NGAY_HHLUC IS NULL OR H.NGAY_HHLUC > p_NGAY_CKY)               ;

        -- HDG_DDO_SOGCS_GT
        OPEN p_HDG_DDO_SOGCS_GT FOR
            SELECT DDO.MA_DVIQLY,
                   DDO.ID_QHE,
                   DDO.MA_SOGCS,
                   DDO.MA_DDO,
                   DDO.MA_KHANG,
                   TO_CHAR (DDO.NGAY_HLUC, 'dd/MM/yyyy') AS NGAY_HLUC,
                   DDO.MA_KVUC,
                   DDO.STT,
                   DDO.KY,
                   DDO.THANG,
                   DDO.NAM
              --           , DDO.NGAY_TAO,
              --           DDO.NGUOI_TAO, DDO.NGAY_SUA, DDO.NGUOI_SUA,
              --           DDO.MA_CNANG
              FROM HDG_DDO_SOGCS DDO
             WHERE     MA_DVIQLY = p_MA_DVIQLY
             AND EXISTS (SELECT 1 FROM HDG_QHE_DDO WHERE MA_DVIQLY = DDO.MA_DVIQLY             
             AND NAM = p_NAM
             AND THANG = p_THANG
             AND KY = p_KY
             AND MA_DDO_PHU = DDO.MA_DDO
             AND MA_SOGCS_CHINH <> MA_SOGCS_PHU
             AND MA_SOGCS_CHINH = p_MA_SO_GCS
             AND LOAI_QHE = '40')                                      
             AND NGAY_HLUC = (SELECT MAX(NGAY_HLUC)
                          FROM HDG_DDO_SOGCS
                         WHERE MA_DVIQLY = DDO.MA_DVIQLY
                           AND MA_DDO = DDO.MA_DDO
                           AND NGAY_HLUC<=p_NGAY_CKY
                           AND KY + 12*THANG + 12*12*NAM <= p_KY + 12*p_THANG + 12*12*p_NAM
                       )
                        AND MA_SOGCS <> p_MA_SO_GCS;     

        -- HDG_BBAN_APGIA_GT
        OPEN p_HDG_BBAN_APGIA_GT FOR
            SELECT MA_DVIQLY,
                   ID_BBANAGIA,
                   MA_BBANAGIA,
                   MA_DDO,
                   TO_CHAR (NGAY_HLUC, 'dd/MM/yyyy')  AS NGAY_HLUC,
                   SO_THUTU,
                   DINH_MUC,
                   LOAI_DMUC,
                   LOAI_BCS,
                   TGIAN_BDIEN,
                   MA_NHOMNN,
                   MA_NGIA,
                   MA_NN,
                   TRANG_THAI,
                   SO_HO,
                   MA_CAPDAP--           , DIEN_GIAI, NGAY_TAO,
                            --           NGUOI_TAO, NGAY_SUA, NGUOI_SUA,
                            --           MA_CNANG
                            ,
                   DECODE (THAO_TACDL, 'TDOI_SOHO', 1, 0) AS IS_SOHO
              --,(select COUNT(*) from HDG_DIEM_DO where MA_DVIQLY=H.MA_DVIQLY and MA_DDO=H.MA_DDO and NGAY_HLUC = H.NGAY_HLUC and THAO_TACDL='SOHO') as IS_SOHO
              FROM HDG_BBAN_APGIA H
             WHERE     MA_DVIQLY = p_MA_DVIQLY AND TRANG_THAI = 1
                   AND MA_DDO IN
                           (SELECT MA_DDO_PHU
                              FROM HDG_QHE_DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND NAM = p_NAM
                                   AND THANG = p_THANG
                                   AND KY = p_KY
                                   AND MA_SOGCS_CHINH <> MA_SOGCS_PHU
                                   AND MA_SOGCS_CHINH = p_MA_SO_GCS
                                   AND LOAI_QHE = '40')
                   AND NGAY_HLUC <= p_NGAY_CKY;

        -- HDG_VITRI_DDO_GT
        OPEN p_HDG_VITRI_DDO_GT FOR
            SELECT H.MA_DVIQLY,
                   H.ID_VITRI_DDO,
                   TO_CHAR (H.NGAY_HLUC, 'dd/MM/yyyy') AS NGAY_HLUC,
                   H.MA_DDO,
                   H.MA_TRAM,
                   H.MA_LO,
                   H.MA_TO,
                   H.PHA,
                   H.SO_COT,
                   H.SO_HOP
              --           , H.NGAY_TAO, H.NGUOI_TAO,
              --           H.NGAY_SUA, H.NGUOI_SUA, H.MA_CNANG
              FROM HDG_VITRI_DDO H
             WHERE     MA_DVIQLY = p_MA_DVIQLY
                   AND MA_DDO IN
                           (SELECT MA_DDO_PHU
                              FROM HDG_QHE_DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND NAM = p_NAM
                                   AND THANG = p_THANG
                                   AND KY = p_KY
                                   AND MA_SOGCS_CHINH <> MA_SOGCS_PHU
                                   AND MA_SOGCS_CHINH = p_MA_SO_GCS
                                   AND LOAI_QHE = '40')
                   AND NGAY_HLUC <= p_NGAY_CKY                                   
               AND (NGAY_HHLUC IS NULL OR NGAY_HHLUC > p_NGAY_CKY)               ;

        OPEN p_HDG_QHE_DDO_TP FOR
            SELECT MA_DDO_PHU, KY_P
              FROM HDG_QHE_DDO
             WHERE     MA_DVIQLY = p_MA_DVIQLY
                   AND NAM = p_NAM
                   AND THANG = p_THANG
                   AND KY = p_KY
                   AND (   MA_SOGCS_CHINH = p_MA_SO_GCS
                        OR MA_DDO_CHINH IN
                               (SELECT MA_DDO_PHU
                                  FROM HDG_QHE_DDO
                                 WHERE     MA_DVIQLY = p_MA_DVIQLY
                                       AND NAM = p_NAM
                                       AND THANG = p_THANG
                                       AND KY = p_KY
                                       AND LOAI_QHE = '40'
                                       AND MA_SOGCS_CHINH = p_MA_SO_GCS
                                       AND MA_SOGCS_CHINH <> MA_SOGCS_PHU))
                   AND LOAI_QHE <> '40'
                   AND LOAI_QHE <> '32';

        OPEN p_HDG_QHE_DDO_BQ FOR
            SELECT MA_DDO_PHU
              FROM HDG_QHE_DDO
             WHERE     MA_DVIQLY = p_MA_DVIQLY
                   AND NAM = p_NAM
                   AND THANG = p_THANG
                   AND KY = p_KY
                   AND MA_SOGCS_CHINH = p_MA_SO_GCS
                   AND LOAI_QHE = '32'
            UNION
            SELECT MA_DDO_CHINH
              FROM HDG_QHE_DDO
             WHERE     MA_DVIQLY = p_MA_DVIQLY
                   AND NAM = p_NAM
                   AND THANG = p_THANG
                   AND KY = p_KY
                   AND MA_SOGCS_PHU = p_MA_SO_GCS
                   AND LOAI_QHE = '32'
            UNION
            SELECT MA_DDO_PHU
              FROM HDG_QHE_DDO
             WHERE     MA_DVIQLY = p_MA_DVIQLY
                   AND NAM = p_NAM
                   AND THANG = p_THANG
                   AND KY = p_KY
                   AND LOAI_QHE = '32'
                   AND MA_DDO_CHINH IN
                           (SELECT MA_DDO_CHINH
                              FROM HDG_QHE_DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND NAM = p_NAM
                                   AND THANG = p_THANG
                                   AND KY = p_KY
                                   AND MA_SOGCS_PHU = p_MA_SO_GCS
                                   AND LOAI_QHE = '32');

        OPEN p_HDG_QHE_DDO_GT FOR
            SELECT MA_DDO_PHU
              FROM HDG_QHE_DDO
             WHERE     MA_DVIQLY = p_MA_DVIQLY
                   AND NAM = p_NAM
                   AND THANG = p_THANG
                   AND KY = p_KY
                   AND MA_SOGCS_CHINH <> MA_SOGCS_PHU
                   AND MA_SOGCS_CHINH = p_MA_SO_GCS
                   AND LOAI_QHE = '40';
    OPEN p_HDG_DDO_GTRU FOR
            SELECT H.MA_DVIQLY,
                   H.MA_DDO,
                   H.MA_KHANG,
                   H.TY_LE,
                   H.LOAI_DMUC,
                   H.MANHOM_KHANG
              FROM HDG_DDO_GTRU H
             WHERE     MA_DVIQLY = p_MA_DVIQLY
                   AND 
                   ( MA_DDO IN
                           (SELECT DDO.MA_DDO
                              FROM HDG_DDO_SOGCS DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND MA_SOGCS = p_MA_SO_GCS
                                   AND NGAY_HLUC <= p_NGAY_CKY                                   
                                    AND (NGAY_HHLUC IS NULL OR NGAY_HHLUC > p_NGAY_CKY)
                                    AND KY + 12 * THANG + 12 * 12 * NAM <= p_KY + 12 * p_THANG + 12 * 12 * p_NAM
                                   AND NOT EXISTS
                                           (SELECT 1
                                              FROM HDG_QHE_DDO
                                             WHERE     MA_DVIQLY =
                                                           p_MA_DVIQLY
                                                   AND NAM = p_NAM
                                                   AND THANG = p_THANG
                                                   AND KY = p_KY
                                                   AND MA_SOGCS_CHINH <>
                                                           MA_SOGCS_PHU
                                                   AND MA_SOGCS_PHU =
                                                           p_MA_SO_GCS
                                                   AND MA_DDO_PHU =
                                                           DDO.MA_DDO
                                                   AND LOAI_QHE = '40'))                                                   
                  or
                     MA_DDO IN
                           (SELECT MA_DDO_PHU
                              FROM HDG_QHE_DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND NAM = p_NAM
                                   AND THANG = p_THANG
                                   AND KY = p_KY
                                   AND MA_SOGCS_CHINH <> MA_SOGCS_PHU
                                   AND MA_SOGCS_CHINH = p_MA_SO_GCS
                                   AND LOAI_QHE = '40')
                 )
                   AND KY + 12 * THANG + 12 * 12 * NAM <=
                                       p_KY + 12 * p_THANG + 12 * 12 * p_NAM
                   AND TRANG_THAI=1;
OPEN p_HDG_DDO_TDOIGCS FOR
            SELECT H.MA_DVIQLY, H.MA_KHANG, H.MA_DDO, H.MA_SOGCS, H.NGAY_GCTH, H.THANG_TH, H.NAM_TH, H.NGAY_GC12,NVL (H.DUCSO_NGC12, 0) DUCSO_NGC12,
                   NVL (H.XNHAN_NGC12,0) XNHAN_NGC12
              FROM HDG_DDO_TDOIGCS H
             WHERE     MA_DVIQLY = p_MA_DVIQLY
                AND NAM_TH=p_NAM               
                   AND MA_DDO IN
                           (SELECT DDO.MA_DDO
                              FROM HDG_DDO_SOGCS DDO
                             WHERE     MA_DVIQLY = p_MA_DVIQLY
                                   AND MA_SOGCS = p_MA_SO_GCS
                                   AND NGAY_HLUC <= p_NGAY_CKY                                   
                                    AND (NGAY_HHLUC IS NULL OR NGAY_HHLUC > p_NGAY_CKY)
                                    AND KY + 12 * THANG + 12 * 12 * NAM <= p_KY + 12 * p_THANG + 12 * 12 * p_NAM
                                   AND NOT EXISTS
                                           (SELECT 1
                                              FROM HDG_QHE_DDO
                                             WHERE     MA_DVIQLY =
                                                           p_MA_DVIQLY
                                                   AND NAM = p_NAM
                                                   AND THANG = p_THANG
                                                   AND KY = p_KY
                                                   AND MA_SOGCS_CHINH <>
                                                           MA_SOGCS_PHU
                                                   AND MA_SOGCS_PHU =
                                                           p_MA_SO_GCS
                                                   AND MA_DDO_PHU =
                                                           DDO.MA_DDO
                                                   AND LOAI_QHE = '40'))
                   --AND XAC_NHAN=1
                   ;

    END;