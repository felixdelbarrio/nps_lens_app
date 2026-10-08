"""Corporate identity shared by reports and generated browser assets."""

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
EMAIL_SIGNATURE = (
    '<table role="presentation" cellspacing="0" cellpadding="0" style="margin-top:28px;border-top:1px solid #d3d8e0;width:100%">'
    '<tr><td style="padding:18px 14px 0 0;width:58px;vertical-align:middle;'
    f'font:700 28px \'{BRAND["font"]}\',Arial,sans-serif;color:#070e46">{BRAND["initiative"]}</td>'
    '<td style="padding:18px 0 0;vertical-align:middle;'
    f'font:12px \'{BRAND["font"]}\',Arial,sans-serif;color:#52627a">'
    f'{BRAND["initiative_credit"]}<br><strong style="color:#070e46">{BRAND["initiative_name"]}</strong>'
    "</td></tr></table>"
)

SIGNATURE_CSS = """
.initiative-signature{display:flex;align-items:center;gap:14px;margin-top:18px;padding-top:16px;border-top:1px solid rgba(255,255,255,.18);position:relative;z-index:1}
.initiative-signature img,.bia-logo{display:block;flex:0 0 50px;width:50px;height:36px;object-fit:contain}
.bia-logo{background:var(--asset-bia-logo) center/contain no-repeat}
.initiative-signature div{display:grid;gap:3px;min-width:0}
.initiative-signature div>span{font:400 10px/1.4 var(--font-ui);letter-spacing:.08em;color:#a6c9eb}
.initiative-signature strong{font:500 12px/1.45 var(--font-ui);color:#fff}
"""
