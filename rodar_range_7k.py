from datetime import datetime, timedelta
import time
import report_7k_partners as R

# ✅ Use DD/MM/YYYY (igual o site)
PERIODO_INICIO = "18/01/2026"
PERIODO_FIM = "19/01/2026"
INTERVALO_ESPERA = 10  # segundos


def parse_ddmmyyyy(s: str) -> datetime:
    return datetime.strptime(s, "%d/%m/%Y")


def fmt_ddmmyyyy(d: datetime) -> str:
    return d.strftime("%d/%m/%Y")


def daterange(inicio: str, fim: str):
    di = parse_ddmmyyyy(inicio)
    df = parse_ddmmyyyy(fim)
    cur = di
    while cur <= df:
        yield cur
        cur += timedelta(days=1)


def main():
    fim_dt = parse_ddmmyyyy(PERIODO_FIM)

    for d in daterange(PERIODO_INICIO, PERIODO_FIM):
        day = fmt_ddmmyyyy(d)
        print(f"\n=== Rodando captura para {day} ===")

        # Ajusta datas no módulo do report
        R.DATA_INICIO = day
        R.DATA_FIM = day

        try:
            df = R.capturar_report_7k()
            if df is not None and not df.empty:
                print(f"✅ OK {day}: {len(df)} linha(s).")
            else:
                print(f"⚠️ Sem dados em {day}.")
        except Exception as e:
            print(f"❌ Erro {day}: {e}")

        # ✅ Evita comparar string vs string (pode dar ruim em outros formatos)
        if d.date() != fim_dt.date():
            print(f"Aguardando {INTERVALO_ESPERA}s para a próxima data...")
            time.sleep(INTERVALO_ESPERA)

    print("\n=== Finalizado ===")


if __name__ == "__main__":
    main()
