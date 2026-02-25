"""Descarga la base de datos GeoIP para Camoufox"""
import asyncio
from camoufox import AsyncCamoufox

async def download():
    print("Descargando base de datos GeoIP...")
    await AsyncCamoufox.download_geoip()
    print("✓ GeoIP descargado exitosamente")

if __name__ == "__main__":
    asyncio.run(download())
