"""Corporate identity shared by reports and generated browser assets."""

from base64 import b64encode

from nps_lens.platform.resources import resource_root

BRAND = {
    "name": "BBVA Banca de Empresas e Instituciones",
    "initiative": "bIA",
    "initiative_name": "Banca Inteligente y Autónoma",
    "initiative_credit": "Impulsado por",
    "newsletter_prefix": "[bIA]",
    "font": "Benton Sans BBVA",
}
BRAND_ASSETS = resource_root() / "assets" / "ppt" / "bbva" / "brand"


def email_signature(image_src: str) -> str:
    return (
        '<table role="presentation" cellspacing="0" cellpadding="0" style="margin-top:28px;border-top:1px solid #d3d8e0;width:100%">'
        '<tr><td style="padding:18px 14px 0 0;width:72px;vertical-align:middle">'
        f'<img src="{image_src}" width="72" height="44" alt="bIA" style="display:block;width:72px;height:44px;border:0"></td>'
        '<td style="padding:18px 0 0;vertical-align:middle;'
        f'font:12px \'{BRAND["font"]}\',Arial,sans-serif;color:#52627a">'
        f'{BRAND["initiative_credit"]}<br><strong style="color:#070e46">{BRAND["initiative_name"]}</strong>'
        "</td></tr></table>"
    )


EMAIL_SIGNATURE = email_signature(
    "data:image/png;base64," + b64encode((BRAND_ASSETS / "bia-email.png").read_bytes()).decode()
)

SIGNATURE_CSS = """
.initiative-signature{display:flex;align-items:center;gap:14px;margin-top:18px;padding-top:16px;border-top:1px solid rgba(255,255,255,.18);position:relative;z-index:1}
.initiative-signature img,.bia-logo{display:block;flex:0 0 64px;width:64px;height:39px;object-fit:contain;}
.bia-logo{background:var(--asset-bia-logo) center/contain no-repeat}
.initiative-signature div{display:grid;gap:3px;min-width:0}
.initiative-signature div>span{font:400 10px/1.4 var(--font-ui);letter-spacing:.08em;color:#a6c9eb}
.initiative-signature strong{font:500 12px/1.45 var(--font-ui);color:#fff}
"""


def email_corporate_logo(image_src: str) -> str:
    return (
        f'<img src="{image_src}" width="230" height="81" alt="{BRAND["name"]}" '
        'style="display:block;width:230px;height:81px;max-width:100%;border:0">'
    )


EMAIL_CORPORATE_LOGO = email_corporate_logo(
    "data:image/png;base64," + b64encode((BRAND_ASSETS / "bbva-bei.png").read_bytes()).decode()
)
