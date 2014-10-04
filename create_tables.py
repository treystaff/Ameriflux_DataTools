import sqlite3
def create_L2_tables(db):
    conn = sqlite3.connect(db)
    c = conn.cursor()

    c.execute('''CREATE TABLE sites
			(ID INTEGER primary key,
			site_name str,
			code_name str)''')

    c.execute('''CREATE TABLE L2
			(ID INTEGER primary key,
			site_id REFERENCES sites(id),
			YEAR int,
			GAP int,
			DTIME double,
      DOY int,
			HRMIN int,
			UST double,
			TA double,
			WD double,
			WS double,
			NEE double,
			FC double,
			SFC double,
			H double,
			SH double,
			LE double,
			SLE double,
			FG double,
			TS1 double,
			TSdepth1 double,
			TS2 double,
      TSdepth2 double,
			PREC double,
			RH double,
			PRESS double,
			CO2 double,
			VDP double,
			SWC1 double,
			SWC2 double,
			Rn double,
			PAR double,
			Rg double,
			Rgdif double,
			PARout double,
      RgOut double,
			Rgl double,
			RglOut double,
			H2O double,
			RE double,
			GPP double,
			CO2top double,
			CO2height double,
			APAR double,
			PARdif double,
			APARpct double,
			ZL double)''')

    conn.commit()
