
import os
from dotenv import load_dotenv
import psycopg2
import kivy
from kivy.config import Config
Config.set('kivy', 'keyboard_mode', 'system')
from kivy.config import Config
Config.set('input', 'wm_pen', '')
from kivy.core.window import Window
Window.softinput_mode = 'pan'
from kivymd.app import MDApp
from kivy.app import App
from kivy.metrics import dp
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.label import Label
from kivy.uix.textinput import TextInput
from kivy.uix.popup import Popup
from kivy.uix.screenmanager import ScreenManager, Screen
from datetime import datetime, timedelta
from kivy.lang import Builder
Builder.load_file("style.kv")
from kivy.uix.boxlayout import BoxLayout
from kivy.uix.button import Button
from kivy.uix.checkbox import CheckBox
from kivy.uix.image import Image
from kivy.graphics import Color,Line, Rectangle
from kivy.uix.scrollview import ScrollView
from kivy.uix.gridlayout import GridLayout
from kivy.core.window import Window
from kivy.uix.anchorlayout import AnchorLayout
from kivy.uix.floatlayout import FloatLayout
from kivy.properties import NumericProperty
from kivy.properties import ListProperty
from kivy.uix.popup import Popup
from kivy.properties import ObjectProperty
from kivy.uix.widget import Widget
from kivymd.uix.pickers.datepicker import MDDatePicker
from kivy.clock import Clock
import locale
from kivy.uix.spinner import Spinner,SpinnerOption
from kivy.factory import Factory
from kivy.uix.dropdown import DropDown
from kivymd.uix.menu import MDDropdownMenu
from kivymd.uix.textfield import MDTextField
from kivymd.uix.list import OneLineListItem
from kivymd.uix.snackbar import Snackbar
from kivy.core.text import Label as CoreLabel
from kivy.properties import StringProperty



#Window.size = (1080, 1920)
def  obtener_conexion_sqlserver():
    # Carga variables de .env
    load_dotenv()

    servidor     = os.getenv("PG_SERVIDOR")
    puerto     = os.getenv("PG_PUERTO", 5432)
    basedatos = os.getenv("PG_BASEDATOS")
    usuario     = os.getenv("PG_USUARIO")
    password = os.getenv("PG_CONTRASEÑA")

    conexion = psycopg2.connect(
        host=servidor,
        port=puerto,
        dbname=basedatos,
        user=usuario,
        password=password
    )
    conexion.autocommit = False
    print("Conexión exitosa a PostgreSQL")
    return conexion


def obtener_usuarios_y_contraseñas():# Obtener usuarios y contraseñas desde la base de datos
    conexion = obtener_conexion_sqlserver()
    cursor = conexion.cursor()
    query = "SELECT Usuario, Contraseña, Rol, Habilitado FROM dbo.usuarios WHERE Habilitado = 'Si'"
    cursor.execute(query)

    columnas = [columna[0] for columna in cursor.description]
    data = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]
    conexion.close()

    users = {}
    for row in data:
        users[row['Usuario']] = {
            'password': row['Contraseña'],
            'role': row['Rol'],
            'estado': row['Habilitado']
        }
    return users
def obtener_datos_usuario(username):# Obtener el responsable del usuario logueado
    conexion = obtener_conexion_sqlserver()
    cursor = conexion.cursor()
    consulta = """
        SELECT 
            u.Responsable,
            e.Establecimiento,
            reu.ID_Establecimiento,
            u.ID_Responsable
        FROM dbo.usuarios u
        INNER JOIN dbo.relacion_establecimiento_usuario reu
            ON u.ID_Responsable = reu.ID_Responsable
        INNER JOIN dbo.establecimientos e
            ON reu.ID_Establecimiento = e.ID_Establecimiento
        WHERE u.Usuario    = ?
          AND u.Habilitado = 'Si'
          AND e.Habilitado = 'Si'
    """
    cursor.execute(consulta, (username,))
    row = cursor.fetchone()
    cursor.close()
    conexion.close()
    if row:
        return row[0], row[1], row[2], row[3]
    else:
        return None, None, None, None
def obtener_preventivos(usuario): #Obtener las tareas del plan preventivo
    fecha_actual = datetime.now()
    conexion = obtener_conexion_sqlserver()
    cursor = conexion.cursor()
    consulta = """
        SELECT DISTINCT
            op.ID_OT_Planmto,
            s.Sector,
            e.Equipo_Estructura,
            r.Rutina,
            op.OT,
            r.ID_Rutina,
            op.Fecha_Control,
            p.Tolerancia
        FROM ot_planmto op
        INNER JOIN planmto p 
            ON op.ID_Planmto = p.ID_Planmto
        INNER JOIN rutinas r 
            ON op.ID_Rutina = r.ID_Rutina
        INNER JOIN equipo_estructura e 
            ON op.ID_Equipo_Estructura = e.ID_Equipo_Estructura
        -- Nuevo join: relaciona equipo con sector
        INNER JOIN relacion_sector_equipo_estructura re 
            ON e.ID_Equipo_Estructura = re.ID_Equipo_Estructura
        INNER JOIN sectores s 
            ON re.ID_Sector = s.ID_Sector
        INNER JOIN usuarios u 
            ON op.ID_Responsable = u.ID_Responsable
        WHERE 
            u.Usuario = ?
          AND op.Habilitado = 'Si'
          AND CAST(op.Fecha_Control AS datetime) <= ?
          AND ? <= DATEADD(
                second, 86399,
                DATEADD(hour, p.Tolerancia, CAST(op.Fecha_Control AS datetime))
              )
    """
    try:
        cursor.execute(consulta, [usuario, fecha_actual, fecha_actual])
        datos = cursor.fetchall()
        for fila in datos:
            ID_OT_Planmto, sector, equipo, rutina, ot, id_rutina, fecha_control, tolerancia = fila
            fecha_limite = fecha_control + timedelta(hours=tolerancia) - timedelta(seconds=1)
            print(f"ID_OT_Planmto: {ID_OT_Planmto}")
            print(f"Registro: {fila}")
            print(f"Fecha_Control: {fecha_control} | Fecha_Limite: {fecha_limite}")
    except Exception as e:
        print(f"Error en la consulta SQL preventivos: {e}")
        datos = []
    finally:
        conexion.close()
    return datos
def generar_ot():#Generar una nueva OT Preventiva al finalizar una tarea.
    try:
        # Año actual completo y sus últimos dos dígitos
        current_year_full = datetime.now().year
        current_year_last_two = str(current_year_full)[-2:]
        
        conexion = obtener_conexion_sqlserver()
        cursor = conexion.cursor()
        
        # Consulta el último OT del año actual en la tabla ot_planmto
        query = """
            SELECT TOP 1 OT
            FROM ot_planmto
            WHERE YEAR(Fecha_Control) = ?
            ORDER BY ID_OT_Planmto DESC
        """
        cursor.execute(query, (current_year_full,))
        row = cursor.fetchone()
        
        if row is None:
            # Si no hay registros para el año actual, empezamos en 1
            n = 1
        else:
            # row[0] Obtiene el valor anterior de OT. Ej: "OT-PREV-3/23"
            ot_anterior = row[0]
            try:
                # Dividir por '-' => ["OT", "PREV", "3/23"]
                partes = ot_anterior.split('-')
                if len(partes) >= 3:
                    # La tercera parte es "3/23", sacamos la parte antes de '/'
                    n_str = partes[2].split('/')[0]  # "3"
                    n = int(n_str) + 1
                else:
                    n = 1
            except Exception as parse_err:
                print(f"Error al parsear OT anterior: {parse_err}")
                n = 1
        
        # Construye el nuevo OT con el contador n y los últimos dos dígitos del año
        nuevo_ot = f"OT-PREV-{n}/{current_year_last_two}"
        
        cursor.close()
        conexion.close()
        
        return nuevo_ot
    except Exception as e:
        print(f"Error al generar OT: {e}")
        return None
def obtener_tareas_por_rutina(id_rutina):#Obtiene lista de tareas asignada por rutinas
    conexion = obtener_conexion_sqlserver()
    cursor = conexion.cursor()
    consulta = """
        SELECT t.ID_Tarea, t.Tarea
        FROM relacion_rutinas_tareas rrt
        INNER JOIN tareas t ON rrt.ID_Tarea = t.ID_Tarea
        WHERE rrt.ID_Rutina = ?
    """
    try:
        cursor.execute(consulta, [id_rutina])
        # Retorna una lista de diccionarios en lugar de tuplas.
        tareas = [{"ID_Tarea": fila[0], "Tarea": fila[1]} for fila in cursor.fetchall()]
    except Exception as e:
        print(f"Error obteniendo tareas por rutina: {e}")
        tareas = []
    conexion.close()
    return tareas
def obtener_avance(ID_OT_Planmto):#Recargar los avances guardados
    conexion = obtener_conexion_sqlserver()
    cursor = conexion.cursor()
    consulta = """
        SELECT Estado, Hora_Inicio, Hora_Fin, Observaciones, Tareas
        FROM otmto_planmto
        WHERE ID_OT_Planmto = ?
    """
    try:
        cursor.execute(consulta, [ID_OT_Planmto])
        registro = cursor.fetchone()
    except Exception as e:
        print(f"Error obteniendo avance: {e}")
        registro = None
    conexion.close()
    return registro
def insertar_avance(ID_OT_Planmto, ot, estado, hora_inicio, hora_fin, observaciones, tareas_concatenadas):#Guarda los avances en otmto_planmto
    try:
        # 0) Verificar que el preventivo está habilitado en ot_planmto
        conn_check = obtener_conexion_sqlserver()
        cur_check = conn_check.cursor()
        cur_check.execute(
            "SELECT Habilitado FROM ot_planmto WHERE ID_OT_Planmto = ?",
            (ID_OT_Planmto,)
        )
        row = cur_check.fetchone()
        conn_check.close()
        if not row or row[0] != "Si":
            Popup(
                title="Error",
                content=Label(text="No puede registrar avance: el preventivo no está habilitado."),
                size_hint=(None, None), size=(300, 200)
            ).open()
            return
        otmto = "MTO - " + ot
        fecha_guardado = datetime.now()
        conexion = obtener_conexion_sqlserver()
        cursor = conexion.cursor()
        sql = """
            INSERT INTO otmto_planmto 
            (ID_OT_Planmto, Estado, Fecha, OTMTO, Hora_Inicio, Hora_Fin,
             Observaciones, Habilitado, Tareas)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (ID_OT_Planmto, estado, fecha_guardado, otmto, hora_inicio,
                  hora_fin, observaciones, "Si", tareas_concatenadas)
        cursor.execute(sql, params)
        conexion.commit()
        conexion.close()
        Popup(title="Éxito", content=Label(text="Guardado exitoso"),
              size_hint=(None, None), size=(300,200)).open()
    except Exception as e:
        Popup(title="Error", content=Label(text=str(e)),
              size_hint=(None, None), size=(300,200)).open()
def actualizar_avance(ID_OT_Planmto, ot, estado, hora_inicio, hora_fin, observaciones, tareas_concatenadas):
    try:
        otmto = "MTO - " + ot
        conexion = obtener_conexion_sqlserver()
        cursor = conexion.cursor()
        sql = """
            UPDATE otmto_planmto
            SET Estado = ?, Hora_Inicio = ?, Hora_Fin = ?, Observaciones = ?, Tareas = ?
            WHERE ID_OT_Planmto = ?
        """
        params = (estado, hora_inicio, hora_fin, observaciones, tareas_concatenadas, ID_OT_Planmto)
        cursor.execute(sql, params)
        conexion.commit()
        conexion.close()
        Popup(title="Éxito", content=Label(text="Registro actualizado"),
              size_hint=(None, None), size=(300,200)).open()
    except Exception as e:
        Popup(title="Error", content=Label(text=str(e)),
              size_hint=(None, None), size=(300,200)).open()
def actualizar_stock_componentes(id_ot_planmto, fecha_movimiento, id_establecimiento, id_responsable, componentes_data):#Registra los consumos en stock
    """
    Actualiza la tabla stock de componentes para el registro de OT especificado.

    Parámetros:
      - id_ot_planmto: Identificador del registro en otmto_planmto.
      - fecha_movimiento: Fecha que se usará para el campo Fecha_movimiento (usualmente el mismo que se guardó en otmto_planmto).
      - id_establecimiento: ID_Establecimiento del usuario.
      - id_responsable: ID_Responsable del usuario.
      - componentes_data: Lista de diccionarios, cada uno con las claves:
            'id': ID_Componente (entero)
            'cantidad': cantidad (decimal)
    Retorna True si la actualización fue exitosa, False en caso de error.
    """
    try:
        # 0) Verificar que el preventivo está habilitado en ot_planmto
        conn_check = obtener_conexion_sqlserver()
        cur_check = conn_check.cursor()
        cur_check.execute(
            "SELECT Habilitado FROM ot_planmto WHERE ID_OT_Planmto = ?",
            (id_ot_planmto,)
        )
        row = cur_check.fetchone()
        conn_check.close()
        if not row or row[0] != "Si":
            Popup(
                title="Error",
                content=Label(text="No puede registrar avance: el preventivo no está habilitado."),
                size_hint=(None, None), size=(300, 200)
            ).open()
        conn = obtener_conexion_sqlserver()
        cursor = conn.cursor()
        
        # Consultar los registros existentes para este registro OT
        sql_select = "SELECT ID_Componente, Cantidad FROM stock WHERE ID_OTMTO_Planmto = ?"
        cursor.execute(sql_select, [id_ot_planmto])
        existing = {row[0]: row[1] for row in cursor.fetchall()}  # Mapea ID_Componente -> Cantidad
        
        # Convertir la lista de nuevos componentes en un diccionario: ID_Componente -> cantidad
        new = {item['id']: item['cantidad'] for item in componentes_data}

        # Procesar inserciones y actualizaciones:
        for comp_id, cantidad in new.items():
            if comp_id in existing:
                # Si ya existe y la cantidad es diferente, actualiza:
                if float(existing[comp_id]) != cantidad:
                    sql_update = "UPDATE stock SET Cantidad = ? WHERE ID_OTMTO_Planmto = ? AND ID_Componente = ?"
                    cursor.execute(sql_update, [cantidad, id_ot_planmto, comp_id])
            else:
                # No existe: insertar nuevo registro
                sql_insert = """
                    INSERT INTO stock 
                    (Fecha_movimiento, Movimiento, ID_Componente, Cantidad, ID_Establecimiento, ID_Responsable, ID_OTMTO_Planmto)
                    VALUES (?, 'S', ?, ?, ?, ?, ?)
                """
                cursor.execute(sql_insert, [fecha_movimiento, comp_id, cantidad, id_establecimiento, id_responsable, id_ot_planmto])
        
        # Procesar eliminaciones: para cada registro existente que ya no esté en la nueva selección
        for comp_id in existing:
            if comp_id not in new:
                sql_delete = "DELETE FROM stock WHERE ID_OTMTO_Planmto = ? AND ID_Componente = ?"
                cursor.execute(sql_delete, [id_ot_planmto, comp_id])
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print("Error actualizando stock de componentes:", e)
        return False
def obtener_componentes_por_rutina(id_rutina):
    
    componentes_dict = {}
    tareas = obtener_tareas_por_rutina(id_rutina)
    print(f" Tareas encontradas para rutina {id_rutina}: {tareas!r}")
    conexion = obtener_conexion_sqlserver()
    cursor = conexion.cursor()
    sql = """
        SELECT rtc.ID_Componente, c.Cod_Comp, c.Componente
        FROM relacion_tareas_componentes rtc
        INNER JOIN componentes c ON rtc.ID_Componente = c.ID_Componente
        WHERE rtc.ID_Tarea = ?
    """
    for tarea in tareas:
        tarea_id = tarea["ID_Tarea"]
       
        try:
            cursor.execute(sql, [tarea_id])
            rows = cursor.fetchall()
            print(f" Filas retornadas para tarea {tarea_id}: {rows!r}")
            for row in rows:
                id_comp, cod, comp = row
                clave = f"{cod} - {comp}"
                if id_comp not in componentes_dict:
                    print(f"Añadiendo componente {id_comp}: {clave}")
                componentes_dict[id_comp] = clave
        except Exception as e:
            print(f" Error obteniendo componentes para tarea {tarea_id}: {e}")
    conexion.close()
    resultado = [(idc, comp_str) for idc, comp_str in componentes_dict.items()]
    print(f" Resultado final de componentes: {resultado!r}")
    return resultado
def obtener_precargados_componentes_db(id_ot_planmto):
    try:
        conn = obtener_conexion_sqlserver()
        cursor = conn.cursor()
        query = """
            SELECT sc.ID_Componente, sc.Cantidad, c.Cod_Comp, c.Componente
            FROM stock sc
            INNER JOIN componentes c ON sc.ID_Componente = c.ID_Componente
            WHERE sc.ID_OTMTO_Planmto = ?
        """
        cursor.execute(query, [id_ot_planmto])
        datos = [
            {
                "id": row[0],
                "cantidad": float(row[1]),
                "nombre": f"{row[2]} - {row[3]}"
            }
            for row in cursor.fetchall()
        ]
        conn.close()
        print("Componentes precargados para ID_OTMTO_Planmto", id_ot_planmto, ":", datos)
        return datos
    except Exception as e:
        print("Error obteniendo componentes precargados:", e)
        return []
def actualizar_avance_otmto(id_ot,ot_codigo,estado,hora_inicio,hora_fin,observaciones,tareas_concatenadas):#Actualiza el registro de avance existente en otmto para la orden id_ot
    
    # 0) Verificar que la OT sigue habilitada
    conn_check = obtener_conexion_sqlserver()
    cur_check = conn_check.cursor()
    cur_check.execute(
        "SELECT Habilitado FROM ot WHERE ID_OT = ?",
        (id_ot,)
    )
    row = cur_check.fetchone()
    conn_check.close()
    if not row or row[0] != "Si":
        Popup(
            title="Error",
            content=Label(text="No puede actualizar avance: la OT no está habilitada."),
            size_hint=(None, None), size=(300, 200)
        ).open()
        return

    # 1) Ejecutar el cambio en otmto
    conn = obtener_conexion_sqlserver()
    cur = conn.cursor()
    sql = """
        UPDATE otmto
           SET Cod_OTMTO       = ?,
               Estado         = ?,
               Observaciones   = ?,
               Hora_Inicio    = ?,
               Hora_Fin       = ?,
               Tarea_realizada = ?
         WHERE ID_OT = ?
    """
    params = (
        ot_codigo,
        estado,
        observaciones,
        hora_inicio,
        hora_fin,
        tareas_concatenadas,
        id_ot
    )
    try:
        cur.execute(sql, params)
        conn.commit()
        Popup(
            title="Éxito",
            content=Label(text="Registro actualizado exitosamente."),
            size_hint=(None, None), size=(300, 200)
        ).open()
    except Exception as e:
        Popup(
            title="Error al actualizar",
            content=Label(text=str(e)),
            size_hint=(None, None), size=(300, 200)
        ).open()
    finally:
        cur.close()
        conn.close()
def obtener_avance_otmto(ID_OT):#Recarga el avance guardado en otmto para la OT indicada.
    
    conn = obtener_conexion_sqlserver()
    cur  = conn.cursor()
    sql = """
        SELECT Estado,
               Hora_Inicio,
               Hora_Fin,
               Observaciones,
               Tarea_realizada
        FROM otmto
        WHERE ID_OT = ?
    """
    try:
        cur.execute(sql, [ID_OT])
        registro = cur.fetchone()
    except Exception as e:
        print(f"Error obteniendo avance: {e}")
        registro = None
    finally:
        cur.close()
        conn.close()
    return registro
def insertar_avance_otmto(
    id_ot,
    id_ot_planmto,
    id_responsable,
    ot_codigo,
    estado,
    hora_inicio,
    hora_fin,
    observaciones,
    tareas_concatenadas
):#Guarda un registro en la tabla otmto para la orden id_ot
    """
    :
    - id_ot_planmto: ID del plan si existe
    - id_responsable: ID del usuario que registra el avance.
    - ot_codigo: código de la OT
    - estado: porcentaje de avance (0-100).
    - hora_inicio, hora_fin: datetime de inicio y fin.
    - observaciones: texto descriptivo de observaciones.
    - tareas_concatenadas: IDs de tareas seleccionadas separados por saltos de línea.
    """
    # 0) Verificar que la OT sigue habilitada
    conn_check = obtener_conexion_sqlserver()
    cur_check = conn_check.cursor()
    cur_check.execute(
        "SELECT Habilitado FROM ot WHERE ID_OT = ?",
        (id_ot,)
    )
    row = cur_check.fetchone()
    conn_check.close()
    if not row or row[0] != "Si":
        Popup(
            title="Error",
            content=Label(text="No puede registrar avance: la OT no está habilitada."),
            size_hint=(None, None), size=(300, 200)
        ).open()
        return

    # 1) Preparar inserción con timestamp
    fecha_guardado = datetime.now()
    conn = obtener_conexion_sqlserver()
    cur = conn.cursor()
    sql = """
        INSERT INTO otmto
          (ID_OT,
           ID_OT_Planmto,
           ID_Responsable,
           Cod_OTMTO,
           Fecha,
           Estado,
           Observaciones,
           Hora_Inicio,
           Hora_Fin,
           Tarea_realizada,
           Habilitado)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        id_ot,
        id_ot_planmto or None,
        id_responsable,
        ot_codigo,
        fecha_guardado,
        estado,
        observaciones,
        hora_inicio,
        hora_fin,
        tareas_concatenadas,
        "Si"
    )
    try:
        cur.execute(sql, params)
        conn.commit()
        Popup(
            title="Éxito",
            content=Label(text="Guardado exitoso."),
            size_hint=(None, None), size=(300, 200)
        ).open()
    except Exception as e:
        Popup(
            title="Error al guardar",
            content=Label(text=str(e)),
            size_hint=(None, None), size=(300, 200)
        ).open()
    finally:
        conn.close()
def actualizar_stock_componentes_ot(id_ot, fecha_movimiento, id_establecimiento, id_responsable, componentes_data):#Registra los consumos en stock de las OT
   
    try:
        # 0) Verificar que el preventivo está habilitado en ot_planmto
        conn_check = obtener_conexion_sqlserver()
        cur_check = conn_check.cursor()
        cur_check.execute(
            "SELECT Habilitado FROM ot WHERE ID_OT = ?",
            (id_ot,)
        )
        row = cur_check.fetchone()
        conn_check.close()
        if not row or row[0] != "Si":
            Popup(
                title="Error",
                content=Label(text="No puede registrar avance: el preventivo no está habilitado."),
                size_hint=(None, None), size=(300, 200)
            ).open()
        conn = obtener_conexion_sqlserver()
        cursor = conn.cursor()
        
        # Consultar los registros existentes para este registro OT
        sql_select = "SELECT ID_Componente, Cantidad FROM stock WHERE ID_OTMTO = ?"
        cursor.execute(sql_select, [id_ot])
        existing = {row[0]: row[1] for row in cursor.fetchall()}  # Mapea ID_Componente -> Cantidad
        
        # Convertir la lista de nuevos componentes en un diccionario: ID_Componente -> cantidad
        new = {item['id']: item['cantidad'] for item in componentes_data}

        # Procesar inserciones y actualizaciones:
        for comp_id, cantidad in new.items():
            if comp_id in existing:
                # Si ya existe y la cantidad es diferente, actualiza:
                if float(existing[comp_id]) != cantidad:
                    sql_update = "UPDATE stock SET Cantidad = ? WHERE ID_OTMTO = ? AND ID_Componente = ?"
                    cursor.execute(sql_update, [cantidad, id_ot, comp_id])
            else:
                # No existe: insertar nuevo registro
                sql_insert = """
                    INSERT INTO stock 
                    (Fecha_movimiento, Movimiento, ID_Componente, Cantidad, ID_Establecimiento, ID_Responsable, ID_OTMTO)
                    VALUES (?, 'S', ?, ?, ?, ?, ?)
                """
                cursor.execute(sql_insert, [fecha_movimiento, comp_id, cantidad, id_establecimiento, id_responsable, id_ot])
        
        # Procesar eliminaciones: para cada registro existente que ya no esté en la nueva selección
        for comp_id in existing:
            if comp_id not in new:
                sql_delete = "DELETE FROM stock WHERE ID_OTMTO_Planmto = ? AND ID_Componente = ?"
                cursor.execute(sql_delete, [id_ot, comp_id])
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print("Error actualizando stock de componentes:", e)
        return False
def obtener_precargados_componentes_db_ot(id_ot):# Obtiene y precarga los datos de componentes guardados.
    try:
        conn = obtener_conexion_sqlserver()
        cursor = conn.cursor()
        query = """
            SELECT sc.ID_Componente, sc.Cantidad, c.Cod_Comp, c.Componente
            FROM stock sc
            INNER JOIN componentes c ON sc.ID_Componente = c.ID_Componente
            WHERE sc.ID_OTMTO = ?
        """
        cursor.execute(query, [id_ot])
        datos = [
            {
                "id": row[0],
                "cantidad": float(row[1]),
                "nombre": f"{row[2]} - {row[3]}"
            }
            for row in cursor.fetchall()
        ]
        conn.close()
        print("Componentes precargados para ID_OTMTO", id_ot, ":", datos)
        return datos
    except Exception as e:
        print("Error obteniendo componentes precargados:", e)
        return []
def format_hora(instance, value):# Función de formateo del TextInput de hora formato HH:MM.
    val = ''.join(c for c in value if c.isdigit())[:4]
    nuevo = ''
    if len(val) >= 2:
        nuevo += val[:2] + ':'
    if len(val) > 2:
        nuevo += val[2:4]
    
    instance.unbind(text=format_hora)
    instance.text = nuevo
    instance.bind(text=format_hora)
    return nuevo
def abrir_datepicker(textinput, date_inicio, hora_inicio, date_fin, hora_fin, validar_fechas):# Función para abrir el Calendario.
    """Abre el datepicker para el TextInput dado."""
    def on_date(instance, value, date_range):
        textinput.text = value.strftime('%d/%m/%Y')
        # Tras cerrar el datepicker, se le da foco al TextInput de hora correspondiente
        Clock.schedule_once(lambda dt: setattr(hora_inicio if textinput == date_inicio else hora_fin, 'focus', True), 0.1)
        validar_fechas()
    date_picker = MyDatePicker()
    date_picker.bind(on_save=on_date)
    date_picker.ids.ok_button.text = "Aceptar"
    date_picker.ids.cancel_button.text = "Cancelar"
    date_picker.open()
def build_fecha_hora(fecha_default, hora_default, label_text, abrir_datepicker_func, validar_fechas_func):# Función para construir el widget de fecha/hora.
    """
    Crea y retorna un BoxLayout que contiene:
      - Un Label para el título (20 dp alto).
      - Un TextInput para la fecha (30 dp alto), ocupa 30% del ancho padre y ajusta su texto.
      - Un TextInput para la hora (30 dp alto), ocupa 30% del ancho padre y ajusta su texto.
      - Un Label para mensajes de validación (20 dp alto).
    El contenedor tiene 100 dp de alto total.
    """
    # Contenedor: 30% del ancho del padre, 100 dp de alto fijo
    col = BoxLayout(
        orientation='vertical',
        size_hint=(0.3, None),
        height=dp(100),
        spacing=dp(2),
    )

    # Título
    title_lbl = Label(
        text=label_text,
        font_size=dp(12),
        size_hint_y=None,
        height=dp(20),
        halign='center',
        valign='middle',
    )
    
    title_lbl.bind(size=title_lbl.setter('text_size'))
    col.add_widget(title_lbl)

    #  TextInput de fecha
    txt_fecha = TextInput(
        text=fecha_default or '',
        multiline=False,
        size_hint=(1, None),
        height=dp(30),
        halign='center',
    )
    # Al tocar, abre el calendario
    txt_fecha.bind(
        on_touch_down=lambda inst, touch: abrir_datepicker_func(inst)
        if inst.collide_point(*touch.pos) else None
    )
    # Ajuste automático de font_size al ancho
    txt_fecha.bind(
        size=lambda inst, *_: setattr(inst, 'font_size', inst.width * 0.1)
    )
    col.add_widget(txt_fecha)

    # TextInput de hora
    txt_hora = TextInput(
        text=hora_default or '',
        multiline=False,
        size_hint=(1, None),
        height=dp(30),
        halign='center',
    )
    txt_hora.bind(text=format_hora)
    txt_hora.bind(
        size=lambda inst, *_: setattr(inst, 'font_size', inst.width * 0.1)
    )
    col.add_widget(txt_hora)

    # Etiqueta de mensaje de validación
    msg_lbl = Label(
        text='',
        color=(1, 0, 0, 1),
        font_size=dp(10),
        size_hint_y=None,
        height=dp(20),
        halign='center',
        valign='middle',
    )
    msg_lbl.bind(size=msg_lbl.setter('text_size'))
    col.add_widget(msg_lbl)

    return col, txt_fecha, txt_hora, msg_lbl
def obtener_tareas(): #Obtiene ID_Tarea y Tarea desde la bd.
    conn = obtener_conexion_sqlserver()
    cur = conn.cursor()
    cur.execute("SELECT ID_Tarea, Tarea FROM tareas ORDER BY Tarea")
    datos = cur.fetchall()  # debe ser lista de (ID_Tarea, Tarea)
    conn.close()
    return datos
    print(obtener_tareas())
def obtener_componentes():#Obtiene ID_Componente y Componente desde la bd.
    conn = obtener_conexion_sqlserver()
    cur = conn.cursor()
    cur.execute("SELECT ID_Componente, Componente FROM componentes ORDER BY Componente")
    rows = cur.fetchall()
    conn.close()
    return rows


class LoginScreen(Screen): #Ventana principal de incio.
    def on_checkbox_active(self, checkbox, value): # Si el checkbox está activo, se muestra la contraseña; de lo contrario, se oculta.
        self.ids.password_input.password = not value
    
    def on_login(self):#Logueo
        
        try:
            users = obtener_usuarios_y_contraseñas()
            if users is None:
                
                raise Exception("No se pudo conectar al servidor")
        except Exception as e:
            self._popup_error_conexion(str(e))
            return

        
        username = self.ids.username_input.text
        password = self.ids.password_input.text

        if username in users and users[username]["password"] == password:
            role = users[username]["role"]
            responsable, establecimiento, id_establecimiento, id_responsable = obtener_datos_usuario(username)
            if role == "Mantenimiento":
                self.manager.current = "mantenimiento"
                self.manager.get_screen("mantenimiento").set_user_data(
                    username, responsable, establecimiento,
                    id_establecimiento, id_responsable
                )
                
            elif role == "Operario":
                self.manager.current = "mantenimiento"
                self.manager.get_screen("mantenimiento").set_user_data(
                    username, responsable, establecimiento,
                    id_establecimiento, id_responsable
                )
            else:
                Popup(
                    title="Error",
                    content=Label(text=f"Rol no reconocido: {role}"),
                    size_hint=(None, None), size=(400, 200)
                ).open()
        else:
            Popup(
                title="Error",
                content=Label(text="Usuario o contraseña incorrectos."),
                size_hint=(None, None), size=(400, 200)
            ).open()

    def _popup_error_conexion(self, mensaje):
        from kivy.uix.boxlayout import BoxLayout
        from kivy.metrics import dp

        content = BoxLayout(orientation='vertical', padding=dp(10), spacing=dp(10))
        content.add_widget(Label(
            text="No se pudo conectar al servidor.\n Intente nuevamente.",
            halign='center'
        ))
        btns = BoxLayout(spacing=dp(10), size_hint_y=None, height=dp(40))
        btn_reintentar = Button(text="Reintentar")
        btn_cancelar   = Button(text="Cancelar")
        btns.add_widget(btn_reintentar)
        btns.add_widget(btn_cancelar)
        content.add_widget(btns)

        popup = Popup(
            title="Error de conexión",
            content=content,
            size_hint=(None, None),
            size=(dp(300), dp(180)),
            auto_dismiss=False
        )
        # Al reintentar, cerramos y volvemos a intentar el login
        btn_reintentar.bind(on_release=lambda *a: (popup.dismiss(), self.on_login()))
        btn_cancelar  .bind(on_release=popup.dismiss)
        popup.open()


class MantenimientoScreen(Screen): #Ventana 1 para ingresar a "Preventivos" o "Orden de Trabajo"
    def set_user_data(self, usuario, responsable, establecimiento, id_establecimiento, id_responsable): #Obtiene datos del usuario logueado y carga las etiquetas
        self.usuario = usuario
        self.responsable = responsable
        self.establecimiento = establecimiento
        self.id_establecimiento = id_establecimiento
        self.id_responsable = id_responsable  
        self.ids.user_info_label.text = (
            f"Usuario: {self.usuario}\n"
            f"Responsable: {self.responsable}\n"
            f"Establecimiento: {self.establecimiento}"
        )
    def _update_rect(self, instance, value):
        self.bg_rect.pos = instance.pos
        self.bg_rect.size = instance.size
    def __init__(self, usuario="", responsable="", establecimiento="", **kwargs):
        super().__init__(**kwargs)
        self.usuario = usuario
        self.responsable = responsable
        self.establecimiento = establecimiento
    def cerrar_sesion(self, instance):
        MDApp.get_running_app().stop()

    def abrir_preventivos(self, instance=None):#Abre ventana de Preventivos
        
        if not self.manager.has_screen("preventivos"):
            self.manager.add_widget(PreventivosScreen(
                usuario=self.usuario,
                responsable=self.responsable,
                establecimiento=self.establecimiento,
                id_establecimiento=self.id_establecimiento,  
                id_responsable=self.id_responsable,
                name="preventivos"
            ))
        preventivos_screen = self.manager.get_screen("preventivos")
        preventivos_screen.cargar_datos(self.usuario)
        self.manager.current = "preventivos"

    def abrir_ordenes_trabajo(self): #Abre ventana de Ordenes de Trabajo
        
        if not self.manager.has_screen('ordenes_trabajo'):
            self.manager.add_widget(OrdenesTrabajoScreen(name='ordenes_trabajo'))
        ot_screen = self.manager.get_screen('ordenes_trabajo')

        #Pasaje de datos del usuario logueado
        ot_screen.set_user_data(
            self.usuario,
            self.responsable,
            self.establecimiento,
            self.id_establecimiento,
            self.id_responsable
        )        
        ot_screen.abrir()

class BorderedAnchorLayout(AnchorLayout):#Aplica el bordes a la tabla de tareas y checks de realizó.
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
class BorderedLabel(Label):#Aplica el bordes a los encabezados de la tabla de tareas y checks de realizó.
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

class BorderedButton(Button):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
class ColorProgressBar(Widget): #Color de la barra de estado de avance
    value = NumericProperty(0)
    max = NumericProperty(100)
    
    bar_color = ListProperty([1, 0, 0, 1])
    
    def on_value(self, instance, value):
        progress = min(max(value / float(self.max), 0), 1)
        if progress <= 0.5:
            fraction = progress / 0.5
            self.bar_color = [1, fraction, 0, 1]
        else:
            fraction = (progress - 0.5) / 0.5
            self.bar_color = [1 - fraction, 1, 0, 1]

class ComponentesModal(Popup): #Ventana modal para seleccionar los componentes utilizados y sus cantidades
    """   
    Permite agregar filas (cada fila tiene un Spinner para seleccionar un componente,
    un TextInput para cantidad y un botón 'X' para eliminar la fila). El Spinner se actualiza
    para no mostrar opciones ya seleccionadas.
    """
    def __init__(self, componentes_disponibles, componentes_precargados=None, **kwargs):
        # Guarda datos iniciales
        self.componentes_disponibles = componentes_disponibles[:]  # Copia de la lista completa
        self.seleccionados = []  # Lista para guardar IDs seleccionados (opcional)
        self.filas = []  # Lista de filas (objetos ComponenteRow)
        super().__init__(**kwargs)
        
                
        self.ids.btn_agregar.bind(on_press=self.agregar_fila)
        
        # Agrega filas precargadas o una fila vacía
        if componentes_precargados:
            for comp in componentes_precargados:
                self.agregar_fila(valores=comp)
        else:
            self.agregar_fila()  

    def agregar_fila(self, instance=None, valores=None):
        # Si ya hay tantas filas como opciones disponibles, no se agrega nada.
        if len(self.filas) >= len(self.componentes_disponibles):
            return
        # Se crea la fila (se asume que ComponenteRow está definida e importada)
        fila = ComponenteRow(self.componentes_disponibles, valores)
        # Vincula eventos para actualizar y eliminar la fila
        fila.bind(on_update=lambda *args: self._fila_actualizada())
        fila.bind(on_eliminar=lambda *args: self.eliminar_fila(fila))
        # Agrega la fila al layout del KV
        self.ids.filas_layout.add_widget(fila)
        self.filas.append(fila)
        self.actualizar_spinners()
        self.verificar()

    def eliminar_fila(self, fila):
        if fila in self.filas:
            self.ids.filas_layout.remove_widget(fila)
            self.filas.remove(fila)
            self.actualizar_spinners()
            self.verificar()

    def _fila_actualizada(self):
        self.actualizar_spinners()
        self.verificar()

    def actualizar_spinners(self):
        """
        Recalcula y actualiza los valores de cada Spinner para que solo se muestren las opciones
        disponibles (excluyendo las ya seleccionadas en otras filas, salvo la opción actual).
        """
        seleccionados_global = set()
        for fila in self.filas:
            if fila.spinner.text_id is not None:
                seleccionados_global.add(fila.spinner.text_id)
        for fila in self.filas:
            actual = fila.spinner.text_id
            opciones_disponibles = []
            for opcion in self.componentes_disponibles:
                opcion_id, opcion_label = opcion
                if opcion_id == actual or opcion_id not in (seleccionados_global - {actual}):
                    opciones_disponibles.append(opcion_label)
            fila.spinner.values = opciones_disponibles

    def verificar(self, *args):
        """
        Verifica que en cada fila se haya seleccionado un componente y se haya ingresado una cantidad válida.
        Habilita el botón "Volver" si todas las filas son válidas.
        """
        valido = True
        for fila in self.filas:
            if not fila.es_valido():
                valido = False
                break
        self.ids.btn_volver.disabled = not valido

    def obtener_datos(self):
        """
        Retorna una lista de diccionarios con los datos de cada fila:
        [{'id': ID_Componente, 'cantidad': cantidad (float)}, ...]
        """
        datos = []
        for fila in self.filas:
            datos.append({
                'id': fila.spinner.text_id,
                'cantidad': fila.obtener_cantidad()
            })
        return datos
    def todos_validos(self):
        """
        Devuelve True si todas las filas cumplen es_valido().
        Esto alimenta el disabled: not root.todos_validos() del KV.
        """
        # self.filas es la lista donde guardas cada ComponenteRow
        return all(fila.es_valido() for fila in getattr(self, 'filas', []))
    
class ScalableSpinnerOption(SpinnerOption):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        Clock.schedule_once(lambda dt: self._adjust_font(), 0)
        self.bind(
            size=lambda *a: self._adjust_font(),
            text=lambda *a: self._adjust_font()
        )

    def _adjust_font(self):
        max_fs, min_fs = dp(16), dp(12)
        text = self.text or ""
        label = CoreLabel(text=text, font_size=max_fs)
        label.refresh()
        text_w = label.texture.size[0]
        avail_w = self.width * 0.9
        if text_w <= avail_w:
            self.font_size = max_fs
        else:
            new_fs = max(min_fs, max_fs * (avail_w / text_w))
            self.font_size = new_fs

class ComponenteRow(BoxLayout):
    
    def __init__(self, opciones, valores=None, **kwargs):
        super().__init__(**kwargs)
        

        self.opciones = opciones
        self.valores  = valores or {}

        # 1) Buscamos los widgets que creó el KV
        self.spinner      = self.ids.spinner
        # after self.spinner = self.ids.spinner
        self.spinner.option_cls = ScalableSpinnerOption

        self.txt_cantidad = self.ids.txt_cantidad
        self.btn_eliminar = self.ids.btn_eliminar

        # 2) Forzamos siempre la existencia de text_id
        self.spinner.text_id = None

        # 3) Rellenamos el spinner y bind
        self.spinner.values = [lbl for (_id, lbl) in opciones]
        self.spinner.bind(text=self._on_spinner_text)

        self.spinner.bind(text=self._ajustar_fuente, size=self._ajustar_fuente)
        
        # 2) Ajuste inicial tras creación (espera a que width > 0)
        Clock.schedule_once(lambda dt: self._ajustar_fuente(self.spinner, None), 0)
        # inicializar con un tamaño razonable
        #self.spinner.font_size = max(12, self.spinner.width * 0.05)

        # 4) Bind del textinput
        self.txt_cantidad.bind(text=lambda inst, val: self.dispatch("on_update"))

        # 5) Botón de “X”
        self.btn_eliminar.bind(on_release=lambda inst: self.dispatch("on_eliminar"))

        # 6) Registro de eventos
        self.register_event_type("on_update")
        self.register_event_type("on_eliminar")

        # 7) Si viene valor precargado, lo aplicamos
        if "id" in self.valores:
            sel = self.valores["id"]
            for cid, lbl in opciones:
                if cid == sel:
                    self.spinner.text    = lbl
                    self.spinner.text_id = cid
                    break

        if "cantidad" in self.valores:
            self.txt_cantidad.text = str(self.valores["cantidad"])
    def _ajustar_fuente(self, spinner, *args):
        text = spinner.text or ""
        max_fs = dp(16)
        min_fs = dp(12)

        # Medimos el texto a tamaño máximo
        label = CoreLabel(text=text, font_size=max_fs)
        label.refresh()

        # ancho real del texto
        text_w = label.texture.size[0]
        # 90% del ancho del spinner
        avail_w = spinner.width * 0.9

        if text_w <= avail_w:
            spinner.font_size = max_fs
        else:
            # escala lineal y acota
            new_fs = max(min_fs, max_fs * (avail_w / text_w))
            spinner.font_size = new_fs

    def _on_spinner_text(self, spinner, label):
        # Actualizar text_id y disparar on_update
        for cid, lbl in self.opciones:
            if lbl == label:
                spinner.text_id = cid
                break
        self.dispatch("on_update")

    def es_valido(self):
        try:
            qty = float(self.txt_cantidad.text)
            return (self.spinner.text_id is not None) and (qty > 0)
        except:
            return False

    def obtener_cantidad(self):
        try:
            return float(self.txt_cantidad.text)
        except:
            return 0.0

    def on_update(self):
        pass

    def on_eliminar(self):
        pass

class MyDatePicker(MDDatePicker):# Traduce la barra superior (mes y año)
    
    def _update_month_selector(self):
        SPANISH_MONTHS = [
            "Enero", "Febrero", "Marzo", "Abril",
            "Mayo", "Junio", "Julio", "Agosto",
            "Septiembre", "Octubre", "Noviembre", "Diciembre"
        ]
        self.ids.month_selector.text = f"{SPANISH_MONTHS[self.month - 1]} {self.year}"

    # Traduce la parte lateral izquierda
    def _update_left_panel(self):
        self.ids.left_label.text = f"{self.sel_date.strftime('%d')} de {self.ids.month_selector.text}"

class FechaHoraBackend:# Clase que agrupa estas funciones de backend para la funcionalidad de fecha/hora.
    @staticmethod
    def format_hora(instance, value):
        return format_hora(instance, value)

    @staticmethod
    def abrir_datepicker(textinput, date_inicio, hora_inicio, date_fin, hora_fin, validar_fechas):
        abrir_datepicker(textinput, date_inicio, hora_inicio, date_fin, hora_fin, validar_fechas)

    @staticmethod
    def build_fecha_hora(fecha_default, hora_default, label_text, abrir_datepicker_func, validar_fechas_func):
        return build_fecha_hora(fecha_default, hora_default, label_text, abrir_datepicker_func, validar_fechas_func)

class DetalleTareasPopup(Popup): #Ventana de detalle de tareas. Se listan las tareas con los checks para validar.
   
    def on_obs_text(self, new_text: str): #Recuadro de observaciones
        # 1) recortar a 300 si es necesario
        if len(new_text) > 300:
            trimmed = new_text[:300]
            ti = self.ids.obs_input
            ti.unbind(text=self.on_obs_text)
            ti.text = trimmed
            ti.bind(text=self.on_obs_text)
            new_text = trimmed

        # 2) actualizar contador en el label interno del popup
        count = len(new_text)
        self.ids.obs_label.text = f"Observaciones ({count}/300)"

    def show_otras_menu(self, btn_instance): #Menu "Otras" con opciones de Generar OT ó Registrar Tarea.
        # Creamos un dropdown con dos opciones
        dd = DropDown()
        for texto, accion in [
            ("Generar\n  OT", self.crear_orden_trabajo),
            ("Registrar\n Tarea", self.registrar_trabajo_realizado)
        ]:
            item = Button(
                text=texto,
                size_hint_y=None,
                height=dp(40),
                font_size=dp(14)
            )
            # Al pulsar cada botón, cerramos el dropdown y llamamos a la acción
            item.bind(on_release=lambda inst, a=accion: (dd.select(inst.text), a()))
            dd.add_widget(item)

        # Abrimos el dropdown anclado al botón “Otras opciones”
        dd.open(btn_instance)

    def crear_orden_trabajo(self):
        # 1) cerramos el popup
        self.dismiss()
        # 2) navegamos al screen 'crear_ot'
        app = App.get_running_app()
        crear = app.root.get_screen('crear_ot')
        crear.parent_popup = self
        app.root.current = 'crear_ot'
        

    def registrar_trabajo_realizado(self):
        # 1) Cerramos este popup
        self.dismiss()
        # 2) Navegamos al screen 'registrar_tarea'
        app = App.get_running_app()
        mgr = app.root  # asumo que tu ScreenManager es root
        # Recuperamos la pantalla creada
        reg_screen = mgr.get_screen('registrar_tarea')
        # Le asignamos parent_popup para que se reabra al volver
        reg_screen.parent_popup = self
        # 3) Cambiamos de pantalla
        mgr.current = 'registrar_tarea'
    pass

class PreventivosScreen(Screen): #Ventana donde lsita las rutinas de tareas por equipos.
    def __init__(self, usuario, responsable, establecimiento, id_establecimiento, id_responsable, **kwargs):
        super().__init__(**kwargs)
        # se almacenan valores para luego usarlos en métodos
        self.usuario = usuario
        self.responsable = responsable
        self.establecimiento = establecimiento
        self.id_establecimiento = id_establecimiento
        self.id_responsable = id_responsable
        self.componentes_provisionales = None  # Estado provisional para componentes
        
    
    def configurar_validacion_y_progress(self, date_inicio, hora_inicio, date_fin, hora_fin, msg1, msg2, btn_guardar, checkbox_list):
            # Implementa la lógica de validación, similar a lo que tenías en una función:
            def actualizar_guardar(*args):
                if msg1.text == "" and msg2.text == "":
                    btn_guardar.disabled = False
                else:
                    btn_guardar.disabled = True
            
            # Actualizar validación para los widgets de fecha/hora
            date_inicio.bind(text=lambda *x: actualizar_guardar())
            hora_inicio.bind(text=lambda *x: actualizar_guardar())
            date_fin.bind(text=lambda *x: actualizar_guardar())
            hora_fin.bind(text=lambda *x: actualizar_guardar())
            # Actualiza la validación de observaciones (si se usa self.obs_input)
            self.obs_input.bind(text=lambda *x: actualizar_guardar())

            # Actualizar la barra de progreso
            def update_progress(*args):
                total = len(checkbox_list)
                prog = (sum(1 for chk in checkbox_list if chk.active) / total * 100) if total > 0 else 0
                self.progress_bar.value = prog

            for chk in checkbox_list:
                chk.bind(active=lambda *args: actualizar_guardar()) 

    def set_user_data(self, usuario, responsable, establecimiento, id_establecimiento, id_responsable):
        self.usuario = usuario
        self.responsable = responsable
        self.establecimiento = establecimiento
        self.id_establecimiento = id_establecimiento
        self.id_responsable = id_responsable  
        # Actualiza el label definido en el KV
        self.ids.user_info_label.text = (
            f"Usuario: {self.usuario}\n"
            f"Responsable: {self.responsable}\n"
            f"Establecimiento: {self.establecimiento}"
        )

    def cargar_datos(self, usuario):
        # Limpia el grid previamente cargado en el KV
        self.ids.data_grid.clear_widgets()
        headers = ["Sector", "Equipo", "Rutina", "OT", "Tareas"]
        for header in headers:
            cell = Factory.DataCell(text=header)
            self.ids.data_grid.add_widget(cell)
        # Obtiene datos desde la base de datos
        datos = obtener_preventivos(usuario)
        for fila in datos:
            # Extrae los campos de la fila
            ID_OT_Planmto, sector, equipo, rutina, ot, id_rutina, fecha_control, tolerancia = fila
            # Para cada campo, crea un Label
            for valor in [sector, equipo, rutina, ot]:
                cell = Factory.DataCell(text=str(valor))
                self.ids.data_grid.add_widget(cell)
            # Crea un botón "Ir" que al pulsarlo invoca abrir_detalle_tareas
            btn_ir = Factory.DataButton(text="Ir")
            
            btn_ir.bind(on_press=lambda instance, id_ot=ID_OT_Planmto, eq=equipo, ot_val=ot, id_rut=id_rutina: 
                        self.abrir_detalle_tareas(id_ot, eq, ot_val, id_rut))
            self.ids.data_grid.add_widget(btn_ir)
       
    def abrir_detalle_tareas(self, id_ot, equipo, ot, id_rutina):
        # Lógica para consultar datos
        self.ID_OT_Planmto = id_ot
        self.componentes_provisionales = obtener_precargados_componentes_db(self.ID_OT_Planmto)
        ahora = datetime.now()
        registro_avance = obtener_avance(self.ID_OT_Planmto)
        tareas = obtener_tareas_por_rutina(id_rutina)
        self.ot = ot
        self.id_rutina = id_rutina

        # Instanciar el popup definido en KV
        popup = DetalleTareasPopup()
        self.popup = popup
        # --- Actualizar la zona de encabezado ---
        popup.ids["lbl_equipo"].text = equipo
        popup.ids["lbl_ot"].text = f"Solicitud: {ot}"
        # --- Actualizar la zona de tareas y checkboxes ---
        grid = popup.ids.rows_grid
        grid.clear_widgets()
        self.checkbox_list = []  # Guardar referencias a los CheckBox
        for tarea in tareas:
            # Se crea un Label con estilo para la tarea
            tarea_lbl = BorderedLabel(text=str(tarea["Tarea"]), font_size=18,
                                       halign="left", valign="middle", size_hint_x=0.8)
            tarea_lbl.bind(size=tarea_lbl.setter('text_size'))
            grid.add_widget(tarea_lbl)
            # Se crea el contenedor para el CheckBox
            container = BorderedAnchorLayout(anchor_x='center', anchor_y='center', size_hint_x=0.2)
            chk = CheckBox(size_hint=(None, None), size=(30, 30))
            container.add_widget(chk)
            grid.add_widget(container)
            chk.bind(active=lambda inst, val: self.progress_bar.setter('value')(self, 
            sum(1 for c in self.checkbox_list if c.active)/len(self.checkbox_list)*100))
            self.checkbox_list.append(chk)
        # --- Actualizar la barra de progreso ---
        self.progress_bar = popup.ids.progress_bar
        self.progress_bar.value = 0
        # --- Construir la zona de fecha/hora ---
        # Aquí se usan las funciones de backend para crear los widgets de fecha/hora.
        def validar_fechas():
            try:
                f1 = datetime.strptime(date_inicio.text, "%d/%m/%Y")
                f1 = f1.replace(hour=int(hora_inicio.text[:2]), minute=int(hora_inicio.text[3:]))
                f2 = datetime.strptime(date_fin.text, "%d/%m/%Y")
                f2 = f2.replace(hour=int(hora_fin.text[:2]), minute=int(hora_fin.text[3:]))
                ahora_local = datetime.now()
                msg1.text = "" if f1 <= f2 else "Inicio > Fin"
                msg2.text = "" if f2 <= ahora_local else "Fin > ahora"
                btn_guardar.disabled = not (msg1.text == "" and msg2.text == "")
            except Exception as e:
                btn_guardar.disabled = True
                print("Error en validar_fechas:", e)
        # Definición de abrir_dp_wrapper con lambda
        def abrir_dp_wrapper(textinput):
            abrir_datepicker(textinput, date_inicio, hora_inicio, date_fin, hora_fin, validar_fechas)
        # Construir los widgets para "Inicio" y "Fin" usando build_fecha_hora (función del backend)
        col1, date_inicio, hora_inicio, msg1 = build_fecha_hora(
            ahora.strftime("%d/%m/%Y"),
            ahora.strftime("%H:%M"),
            "Inicio",
            abrir_dp_wrapper,
            validar_fechas
        )
        col2, date_fin, hora_fin, msg2 = build_fecha_hora(
            ahora.strftime("%d/%m/%Y"),
            ahora.strftime("%H:%M"),
            "Fin",
            abrir_dp_wrapper,
            validar_fechas
        )
        # Se crea el contenedor de fecha/hora definido en KV:
        date_layout = popup.ids.date_layout
        date_layout.clear_widgets()
        date_layout.add_widget(col1)
        date_layout.add_widget(col2)
        # --- Zona de observaciones ---
        self.obs_input = popup.ids.obs_input
        self.tareas = obtener_tareas_por_rutina(id_rutina)

        comp_options = obtener_componentes_por_rutina(id_rutina)
        # --- Configurar botones del popup ---
        btn_volver    = popup.ids['btn_volver']
        btn_guardar   = popup.ids['btn_guardar']
        btn_registrar = popup.ids['btn_finalizar']
        btn_otras     = popup.ids['btn_otras']

        btn_añadir_comp = popup.ids.btn_añadir_comp
        btn_volver.bind(on_release=lambda instance: popup.dismiss())
        btn_guardar.bind(on_release=lambda instance: self.do_guardar(date_inicio, hora_inicio, date_fin, hora_fin, msg1, msg2,self.tareas))
        btn_registrar.bind(on_release=lambda *_: (self.registrar_preventivo()))
        btn_otras.bind(on_release=lambda btn: popup.show_otras_menu(btn))
        btn_añadir_comp.bind(on_release=lambda instance: self.abrir_modal_componentes(comp_options))
        
           
       
        # Precargar datos de avance, si existen
        if registro_avance is not None:
            estado_guard, hora_ini_guard, hora_fin_guard, obs_guard, tareas_guard = registro_avance
            self.progress_bar.value = float(estado_guard)
            date_inicio.text = hora_ini_guard.strftime("%d/%m/%Y")
            hora_inicio.text = hora_ini_guard.strftime("%H:%M")
            date_fin.text = hora_fin_guard.strftime("%d/%m/%Y")
            hora_fin.text = hora_fin_guard.strftime("%H:%M")
            if obs_guard is not None:
                self.obs_input.text = obs_guard
            tareas_guard_list = tareas_guard.splitlines() if tareas_guard else []
            for chk, tarea in zip(self.checkbox_list, tareas):
                if tarea["Tarea"] in tareas_guard_list:
                    chk.active = True

        # Configurar bindings para validación y actualización del progreso
        date_inicio.bind(text=lambda *x: self.configurar_validacion_y_progress(date_inicio, hora_inicio, date_fin, hora_fin, msg1, msg2, btn_guardar, self.checkbox_list))
        hora_inicio.bind(text=lambda *x: self.configurar_validacion_y_progress(date_inicio, hora_inicio, date_fin, hora_fin, msg1, msg2, btn_guardar, self.checkbox_list))
        date_fin.bind(text=lambda *x: self.configurar_validacion_y_progress(date_inicio, hora_inicio, date_fin, hora_fin, msg1, msg2, btn_guardar, self.checkbox_list))
        hora_fin.bind(text=lambda *x: self.configurar_validacion_y_progress(date_inicio, hora_inicio, date_fin, hora_fin, msg1, msg2, btn_guardar, self.checkbox_list))
        self.obs_input.bind(text=lambda *x: self.configurar_validacion_y_progress(date_inicio, hora_inicio, date_fin, hora_fin, msg1, msg2, btn_guardar, self.checkbox_list))
        for chk in self.checkbox_list:
            chk.bind(active=lambda instance, value: self.configurar_validacion_y_progress(date_inicio, hora_inicio, date_fin, hora_fin, msg1, msg2, btn_guardar, self.checkbox_list))
            chk.bind(active=lambda *args: self.configurar_validacion_y_progress(date_inicio, hora_inicio, date_fin, hora_fin, msg1, msg2, btn_guardar, self.checkbox_list))
        App.get_running_app().detail_popup = popup
        # Abrir el popup
        popup.open()

        def actualizar_guardar(self, *args):
            # Lógica para habilitar o deshabilitar el botón guardar (según validaciones)
            if self.msg1.text == "" and self.msg2.text == "":
                self.btn_guardar.disabled = False
            else:
                self.btn_guardar.disabled = True

        def precargar_datos_avance(self, registro_avance, checkbox_list, tareas):
            """Actualiza los widgets del popup con los datos precargados del avance."""
            if registro_avance is not None:
                estado_guard, hora_ini_guard, hora_fin_guard, obs_guard, tareas_guard = registro_avance
                self.progress_bar.value = float(estado_guard)
                # Actualizar los widgets de fecha/hora (los widgets se obtienen de la zona generada con build_fecha_hora)
                date_inicio.text = hora_ini_guard.strftime("%d/%m/%Y")
                hora_inicio.text = hora_ini_guard.strftime("%H:%M")
                date_fin.text = hora_fin_guard.strftime("%d/%m/%Y")
                hora_fin.text = hora_fin_guard.strftime("%H:%M")
                if obs_guard is not None:
                    self.obs_input.text = obs_guard
                tareas_guard_list = tareas_guard.splitlines() if tareas_guard else []
                for chk, tarea in zip(checkbox_list, tareas):
                    if tarea["Tarea"] in tareas_guard_list:
                        chk.active = True

    def do_guardar(self, date_inicio, hora_inicio, date_fin, hora_fin, msg1, msg2, tareas):
        try:
            f1 = datetime.strptime(date_inicio.text, "%d/%m/%Y")
            f1 = f1.replace(hour=int(hora_inicio.text[:2]), minute=int(hora_inicio.text[3:]))
            f2 = datetime.strptime(date_fin.text, "%d/%m/%Y")
            f2 = f2.replace(hour=int(hora_fin.text[:2]), minute=int(hora_fin.text[3:]))
            self.hora_inicio = f1
            self.hora_fin = f2
        except Exception as ex:
            print("Error al parsear fechas:", ex)
            return
        self.tareas_concatenadas = "\n".join([t["Tarea"] for t, chk in zip(obtener_tareas_por_rutina(self.id_rutina), self.checkbox_list) if chk.active])
        self.guardar_avance(self.ot, self.progress_bar.value, self.hora_inicio, self.hora_fin,
                             self.obs_input.text, self.tareas_concatenadas)

    def volver(self, instance):
        self.manager.current = "mantenimiento"
    
    def abrir_modal_componentes(self, comp_options):
        precargados = self.obtener_precargados_componentes()
        modal = ComponentesModal(comp_options, precargados)
        modal.open()
        modal.bind(on_dismiss=lambda instance: self.actualizar_componentes_provisionales(modal.obtener_datos()))

    def obtener_precargados_componentes(self):
        if self.componentes_provisionales is not None:
            return self.componentes_provisionales
        else:
            self.componentes_provisionales = obtener_precargados_componentes_db(self.ID_OT_Planmto)
            return self.componentes_provisionales

    def actualizar_componentes_provisionales(self, nuevos_componentes):
        self.componentes_provisionales = nuevos_componentes

    def guardar_avance(self, ot, estado, hora_inicio, hora_fin, observaciones, tareas_concatenadas):
        # Validaciones y llamadas a funciones para insertar/actualizar avances en la base de datos.
        if not (hora_inicio and hora_fin):
            Popup(title="Error", content=(Label(text="Debe seleccionar Hora de Inicio y de Fin.",size_hint_y=None, height=dp(60),halign='center', valign='middle',text_size=(Window.width * 0.8 - dp(20), None))),
                  size_hint=(None, None), size=(300,200)).open()
            return
        if hora_inicio >= hora_fin:
            Popup(title="Error", content=(Label(text="La Hora de Inicio debe ser menor que la Hora de Fin.",size_hint_y=None, height=dp(60),halign='center', valign='middle',text_size=(Window.width * 0.8 - dp(20), None))),
                  size_hint=(None, None), size=(300,200)).open()
            return
        if hora_fin > datetime.now():
            Popup(title="Error", content=(Label(text="La Hora de Fin debe ser menor o igual a la fecha y hora actual.",size_hint_y=None, height=dp(60),halign='center', valign='middle',text_size=(Window.width * 0.8 - dp(20), None))),
                  size_hint=(None, None), size=(300,200)).open()
            return
        if not tareas_concatenadas.strip():
            Popup(title="Error", content=(Label(text="Debe seleccionar al menos una tarea.",size_hint_y=None, height=dp(60),halign='center', valign='middle',text_size=(Window.width * 0.8 - dp(20), None))), 
                  size_hint=(None, None), size=(300,200)).open()
            return

        def on_confirm(instance):
            confirm_popup.dismiss()
            if obtener_avance(self.ID_OT_Planmto) is None:
                insertar_avance(self.ID_OT_Planmto, ot, estado, hora_inicio, hora_fin, observaciones, tareas_concatenadas)
            else:
                actualizar_avance(self.ID_OT_Planmto, ot, estado, hora_inicio, hora_fin, observaciones, tareas_concatenadas)
            if self.componentes_provisionales:
                fecha_mov = datetime.now()
                actualizar_stock_componentes(self.ID_OT_Planmto, fecha_mov, self.id_establecimiento, self.id_responsable, self.componentes_provisionales)
            else:
                print("No se han seleccionado componentes; no se actualizará stock de componentes.")

        def on_cancel(instance):
            confirm_popup.dismiss()

        content_layout = BoxLayout(orientation='vertical', spacing=10, padding=dp(10))

        
        content_layout.add_widget(Label(text="¿Desea guardar el avance?",size_hint_y=None, height=dp(60),halign='center', valign='middle',text_size=(Window.width * 0.8 - dp(20), None)))
        btn_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=dp(40), spacing=dp(5))
        btn_si = Button(text="Sí", on_release=on_confirm)
        btn_no = Button(text="No", on_release=on_cancel)
        btn_si.bind(on_press=on_confirm)
        btn_no.bind(on_press=on_cancel)
        btn_layout.add_widget(btn_si)
        btn_layout.add_widget(btn_no)
        content_layout.add_widget(btn_layout)
        confirm_popup = Popup(title="Confirmación", content=content_layout, size_hint=(0.8, None), height=dp(180),auto_dismiss=False)
        confirm_popup.open()

    def registrar_preventivo(self):
        """
        1) Pide confirmación.
        2) Si confirma, marca el preventivo actual como 'No' y obtiene los datos clave.
        3) Consulta planmto; si ya está deshabilitado detiene ahí.
        4) Calcula y actualiza Fecha_Control en planmto, commit intermedio.
        5) Genera nueva OT e inserta en ot_planmto.
        6) Recupera el nuevo ID y lo guarda en planmto.ID_OT_Planmto.
        7) Vuelve a deshabilitar el preventivo original para evitar modificaciones.
        8) Cierra el popup de detalles y vuelve a la pantalla de preventivos recargando sus datos.
        """
        def on_confirm(instance):
            # 1) Cerrar el popup de confirmación
            popup.dismiss()
            print("→ registrar_preventivo: confirmación ACEPTADA")
            # —— 0) Verificar que exista al menos un registro en otmto_planmto
            conn_check = obtener_conexion_sqlserver()
            cur_check = conn_check.cursor()
            cur_check.execute(
                "SELECT COUNT(*) FROM otmto_planmto WHERE ID_OT_Planmto = ?",
                [self.ID_OT_Planmto]
            )
            if cur_check.fetchone()[0] == 0:
                conn_check.close()
                Popup(
                    title="Error",
                    content=Label(text="Debe guardar los cambios antes de finalizar el preventivo."),
                    size_hint=(0.8, None), height=dp(120)
                ).open()
                return
            conn_check.close()

            # —— 1) Deshabilitar el preventivo actual
            
            conn = obtener_conexion_sqlserver()
            cursor = conn.cursor()
            try:
                # 2) Deshabilitar el preventivo actual
                cursor.execute(
                    "UPDATE ot_planmto SET Habilitado = 'No' WHERE ID_OT_Planmto = ?",
                    [self.ID_OT_Planmto]
                )
                conn.commit()
                print(f"    ✔ Preventivo {self.ID_OT_Planmto} marcado como 'No'")

                # 3) Leer datos clave del preventivo
                cursor.execute(
                    "SELECT ID_Responsable, ID_Planmto, ID_Rutina, ID_Equipo_Estructura "
                    "FROM ot_planmto WHERE ID_OT_Planmto = ?",
                    [self.ID_OT_Planmto]
                )
                fila = cursor.fetchone()
                if not fila:
                    raise Exception("No se encontró el preventivo para deshabilitar.")
                id_responsable, id_planmto, id_rutina, id_equipo = fila
                print(f"    → Datos clave: {fila}")

                # 4) Consultar estado y frecuencia en planmto
                cursor.execute(
                    "SELECT Habilitado, Frecuencia FROM planmto WHERE ID_Planmto = ?",
                    [id_planmto]
                )
                habil_plan, frecuencia = cursor.fetchone()
                print(f"    → planmto.habilitado={habil_plan}, frecuencia={frecuencia}")
                if habil_plan == 'No':
                    Popup(
                        title="Info",
                        content=Label(text="Plan ya deshabilitado, no se genera siguiente preventivo."),
                        size_hint=(0.8, None), height=dp(120)
                    ).open()
                    conn.commit()
                    return

                # 5) Calcular y actualizar nueva Fecha_Control
                nueva_fc = datetime.now() + timedelta(hours=frecuencia)
                cursor.execute(
                    "UPDATE planmto SET Fecha_Control = ? WHERE ID_Planmto = ?",
                    [nueva_fc, id_planmto]
                )
                conn.commit()
                print(f"    ✔ planmto Fecha_Control actualizado a {nueva_fc}")

                # 6) Generar e insertar nueva OT en ot_planmto
                nueva_ot = generar_ot()
                cursor.execute(
                    "INSERT INTO ot_planmto "
                    "(ID_Responsable, ID_Rutina, ID_Planmto, ID_Equipo_Estructura, OT, Fecha_Control, Habilitado) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'Si')",
                    [id_responsable, id_rutina, id_planmto, id_equipo, nueva_ot, nueva_fc]
                )
                conn.commit()
                print(f"    ✔ Nueva OT '{nueva_ot}' insertada en ot_planmto")

                # 7) Recuperar nuevo ID_OT_Planmto y actualizar planmto
                cursor.execute("SELECT @@IDENTITY")
                nuevo_id = cursor.fetchone()[0]
                cursor.execute(
                    "UPDATE planmto SET ID_OT_Planmto = ? WHERE ID_Planmto = ?",
                    [nuevo_id, id_planmto]
                )
                conn.commit()
                print(f"    ✔ planmto.ID_OT_Planmto actualizado a {nuevo_id}")

                # 8) Asegurar que el preventivo original queda deshabilitado
                cursor.execute(
                    "UPDATE ot_planmto SET Habilitado = 'No' WHERE ID_OT_Planmto = ?",
                    [self.ID_OT_Planmto]
                )
                conn.commit()
                print(f"    ✔ Preventivo original {self.ID_OT_Planmto} confirmado como 'No'")

                # 9) Mostrar popup de éxito
                exitoso = Popup(
                    title="Registro finalizado con éxito",
                    content=Label(text=f"Se ha generado la nueva orden preventiva:\n{nueva_ot}"),
                    size_hint=(0.8, None), height=dp(140),
                    auto_dismiss=True
                )
                exitoso.open()

                # 10) Cerrar el popup de detalles de tareas si existe
                if hasattr(self, 'popup') and self.popup:
                    self.popup.dismiss()

                # 11) Recargar y volver a la pantalla de preventivos
                prev = self.manager.get_screen('preventivos')
                prev.cargar_datos(self.usuario)
                self.manager.current = 'preventivos'

            except Exception as e:
                Popup(
                    title="Error",
                    content=Label(text=str(e)),
                    size_hint=(0.8, None), height=dp(120)
                ).open()
            finally:
                conn.close()

        def on_cancel(instance):
            popup.dismiss()
            print("→ registrar_preventivo: confirmación CANCELADA")

        # Construcción del popup de confirmación
        content = BoxLayout(orientation='vertical', spacing=dp(10), padding=dp(10))
        content.add_widget(Label(
            text="¿Desea finalizar este preventivo? Guarde los cambios antes de avanzar.",
            size_hint_y=None, height=dp(60),
            halign='center', valign='middle',
            text_size=(Window.width * 0.8 - dp(20), None)
        ))
        btns = BoxLayout(orientation='horizontal', size_hint_y=None, height=dp(40), spacing=dp(10))
        btn_si = Button(text="Sí", size_hint_x=0.5, on_release=on_confirm)
        btn_no = Button(text="No", size_hint_x=0.5, on_release=on_cancel)
        btns.add_widget(btn_si)
        btns.add_widget(btn_no)
        content.add_widget(btns)

        popup = Popup(
            title="Confirmación",
            content=content,
            size_hint=(0.8, None),
            height=dp(180),
            auto_dismiss=False
        )
        popup.open()

class AutoResizeTextField(MDTextField):
    min_font_size = dp(12)   # tope mínimo
    max_font_size = dp(20)   # tamaño base

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # binder para texto y tamaño
        self.bind(text=self._adjust_font,
                  width=self._adjust_font)

    def _adjust_font(self, *args):
        if not self.text:
            self.font_size = self.max_font_size
            return
        max_w = self.width - dp(10)
        fs = self.max_font_size
        core = CoreLabel(text=self.text, font_size=fs)
        core.refresh()
        # reducir hasta que quepa o llegue a min_font_size
        while core.texture.size[0] > max_w and fs > self.min_font_size:
            fs -= dp(1)
            core = CoreLabel(text=self.text, font_size=fs)
            core.refresh()
        self.font_size = fs

class AutoResizeListItem(OneLineListItem):
    min_font_size = dp(12)
    max_font_size = dp(16)

    def on_kv_post(self, base_widget):
        super().on_kv_post(base_widget)
        # ahora self.ids['text_label'] sí existe
        self._label = self.ids.get('text_label')
        # bind a cambios de texto y ancho
        self.bind(text=self._adjust_font, width=self._adjust_font)
        # primer ajuste tras layout
        Clock.schedule_once(lambda dt: self._adjust_font())

    def _adjust_font(self, *args):
        if not self._label or not self.text:
            return
        max_w = self.width - dp(20)
        fs = self.max_font_size
        core = CoreLabel(text=self.text, font_size=fs)
        core.refresh()
        while core.texture.size[0] > max_w and fs > self.min_font_size:
            fs -= dp(1)
            core = CoreLabel(text=self.text, font_size=fs)
            core.refresh()
        self._label.font_size = fs

Factory.register('AutoResizeListItem', cls=AutoResizeListItem)

class CrearOTScreen(Screen): #Ventana de Generar  dentro de Preventivos
    MAX_FILAS = 6

    def __init__(self, **kw):
        super().__init__(**kw)
        # menú base (no se usa directamente)
        self.task_menu = MDDropdownMenu(
            caller=None,
            items=[],
            width_mult=4,
        )
    def _on_comp_spinner_change(self, spinner, val):
            # Obtiene todos los valores de spinner en comps_grid
            spinners = [w for w in self.ids.comps_grid.children if isinstance(w, Spinner)]
            textos = [s.text for s in spinners if s.text]            
            if textos.count(val) > 1:
                Snackbar(text="No puede asignar la misma tarea a dos componentes").open()
                # Reseteo este spinner para que vuelva a estar vacío
                spinner.text = ''
            # Y siempre reevalúo el estado del botón
            self._update_confirm_btn()
    def on_pre_enter(self):
        raw_t = obtener_tareas()
        self.tasks_data = [(row.ID_Tarea, row.Tarea) for row in raw_t]
        self.task_texts = [texto for _, texto in self.tasks_data]

        raw_c = obtener_componentes()
        self.comps_data = [(row[0], row[1]) for row in raw_c]
        self.comp_texts = [texto for _, texto in self.comps_data]

        # Contadores y estado inicial
        self._n_tasks = 0
        self._n_comps = 0
        self.ids.tasks_grid.clear_widgets()
        self.ids.comps_grid.clear_widgets()
        self.ids.obs_input.text = ''
        self.ids.obs_count.text = '0/300'
        

        self.ids.btn_confirm.disabled = True
    def volver(self):
        self.manager.current = 'preventivos'
        if hasattr(self, 'parent_popup'):
            Clock.schedule_once(lambda dt: self.parent_popup.open(), 0)    
    def _handle_task_focus(self, inst, focus):
        # abrir/cerrar menú
        if focus and inst.menu.items:
            inst.menu.open()
        else:
            inst.menu.dismiss()
        # validar selección
        self._validate_task_exact(inst, focus)
    def add_task_row(self):
        if self._n_tasks >= self.MAX_FILAS:
            return
        self._n_tasks += 1

        grid = self.ids.tasks_grid

        lbl = Label(
            text=str(self._n_tasks),
            size_hint_x=0.1, size_hint_y=None, height=dp(35),
            halign='center', valign='middle'
        )
        lbl.bind(size=lambda inst, sz: setattr(inst, 'text_size', inst.size))

        tf = AutoResizeTextField(
            hint_text='Seleccione…',
            size_hint_x=0.8, size_hint_y=None, height=dp(35),
            multiline=False
        )
        tf.selected = False
        tf.invalid = False
        tf.menu = MDDropdownMenu(
            caller=tf,
            items=[],
            width_mult=4,
            max_height=dp(200)
        )
        # bind usando único handler para on_focus
        tf.bind(
            on_focus=self._handle_task_focus,
            text=self._on_task_text
        )

        btn = Button(
            text='X',
            size_hint_x=0.1, size_hint_y=None, height=dp(35)
        )
        btn.bind(height=lambda inst, h: setattr(inst, 'font_size', h * 0.8))
        btn.font_size = btn.height * 0.8
        btn.bind(on_release=lambda *_: self._remove_task_row(lbl, tf, btn))

        grid.add_widget(lbl)
        grid.add_widget(tf)
        grid.add_widget(btn)
        self._update_confirm_btn()

        Clock.schedule_once(lambda dt: setattr(btn, 'font_size', btn.height * 0.8), 0)
    def _on_task_text(self, tf, texto_actual):
        tf.selected = False
        filtro = texto_actual.lower()
        opciones = [t for (_id, t) in self.tasks_data if filtro in t.lower()]
        menu = tf.menu
        menu.dismiss()
        if opciones:
            menu.items = [{
            'viewclass': 'AutoResizeListItem',
            'text': tarea_text,
            'height': dp(35),
            'on_release': lambda x=tarea_text: self._select_task(x, tf)
        } for tarea_text in opciones]
            menu.open()
        else:
            menu.items = []
        self._update_confirm_btn()
    def _validate_task_exact(self, tf, focus):
        if not focus:
            if not tf.selected:
                tf.text = ''
                tf.invalid = True
                Snackbar(text="Seleccione una tarea válida").open()
            else:
                tf.invalid = False
            self._update_confirm_btn()
    def _select_task(self, texto, tf):
        tf.text = texto
        tf.selected = True
        tf.menu.dismiss()
        self._update_confirm_btn()
    def _remove_task_row(self, lbl, tf, btn):
        grid = self.ids.tasks_grid
        # Elimina widgets y decremento contador
        for w in (lbl, tf, btn):
            grid.remove_widget(w)
        self._n_tasks -= 1

        #  Renumerar etiquetas de tareas
        count = 1
        for child in grid.children[::-1]:
            if isinstance(child, Label) and not isinstance(child, Button):
                child.text = str(count)
                count += 1

        #  Renumerar spinners** en comps_grid:
        for widget in self.ids.comps_grid.children:
            if isinstance(widget, Spinner):
                # 3.a) ajustar lista de valores a las nuevas filas de tareas
                widget.values = [str(i) for i in range(1, self._n_tasks + 1)]
                # 3.b) si el texto actual ya no existe, reinícialo
                if widget.text not in widget.values:
                    widget.text = ''

        # 4. Actualizar estado del botón
        self._update_confirm_btn()
    def add_comp_row(self):
        if self._n_comps >= self.MAX_FILAS:
            return
        self._n_comps += 1
        grid = self.ids.comps_grid

        spinner = Spinner(
            text='1',
            values=[str(i) for i in range(1, self._n_tasks+1)],
            size_hint_x=0.1,
            size_hint_y=None,
            height=dp(35),
                       
        )
        spinner.bind(text=lambda *a: self._update_confirm_btn())
        spinner.bind(height=lambda inst, h: setattr(inst, 'font_size', h * 0.8))
        spinner.font_size = spinner.height * 0.8        
        spinner.bind(text=lambda inst, val: self._on_comp_spinner_change(inst, val))
        
        
        tf = MDTextField(
            hint_text='Seleccione…',
            size_hint_x=0.8,
            size_hint_y=None,
            height=dp(35),
            multiline=False
        )
        tf.selected = False
        tf.invalid = False
        tf.menu = MDDropdownMenu(
            caller=tf,
            items=[],
            width_mult=4,
            max_height=dp(200)
        )
        tf.bind(on_focus=lambda inst, focus: inst.menu.open() if focus and inst.menu.items else inst.menu.dismiss())
        tf.bind(text=self._on_comp_text)
        tf.bind(on_focus=self._validate_comp_exact)

        btn = Button(
            text='X',
            size_hint_x=0.1,
            size_hint_y=None,
            height=dp(35),
            font_size=dp(24)                # ← aquí fijas el tamaño de la “X”
        )
        btn.bind(on_release=lambda *_: self._remove_comp_row(spinner, tf, btn))

        grid.add_widget(spinner)
        grid.add_widget(tf)
        grid.add_widget(btn)
        self._update_confirm_btn()

        Clock.schedule_once(lambda dt: setattr(spinner, 'font_size', spinner.height * 0.8), 0)
        Clock.schedule_once(lambda dt: setattr(btn,     'font_size', btn.height     * 0.8), 0)
    def _on_comp_text(self, tf, texto_actual):
        tf.selected = False
        filtro = texto_actual.lower()
        opciones = [c for (_id, c) in self.comps_data if filtro in c.lower()]
        menu = tf.menu
        menu.dismiss()
        if opciones:
            menu.items = [{
                'viewclass': 'OneLineListItem',
                'text': comp_text,
                'height': dp(35),
                'on_release': lambda x=comp_text: self._select_comp(x, tf)
            } for comp_text in opciones]
            menu.open()
        else:
            menu.items = []
        self._update_confirm_btn()
    def _validate_comp_exact(self, tf, focus):
        if not focus and tf.text:
            if not tf.selected:
                tf.text = ''
                tf.invalid = True
                Snackbar(text="Seleccione un componente válido").open()
            else:
                tf.invalid = False
            self._update_confirm_btn()
    def _select_comp(self, texto, tf):
        tf.text = texto
        tf.selected = True
        tf.menu.dismiss()
        self._update_confirm_btn()
    def _remove_comp_row(self, spinner, tf, btn):
        grid = self.ids.comps_grid
        for w in (spinner, tf, btn):
            grid.remove_widget(w)
        self._n_comps -= 1
        self._update_confirm_btn()
    def on_obs_text(self, inst, txt):
        # límite 300
        if len(txt) > 300:
            inst.text = txt[:300]
            txt = inst.text
        # actualizar contador externo
        self.ids.obs_count.text = f"{len(txt)}/300"
        self._update_confirm_btn()
    def _update_confirm_btn(self):
        # Validar que todas las tareas y componentes estén seleccionados
        task_fields = [w for w in self.ids.tasks_grid.children if isinstance(w, MDTextField)]
        comp_fields = [w for w in self.ids.comps_grid.children if isinstance(w, MDTextField)]
        all_tasks_valid = bool(task_fields) and all(getattr(w, 'selected', False) for w in task_fields)
        all_comps_valid = all(getattr(w, 'selected', False) for w in comp_fields)
        self.ids.btn_confirm.disabled = not (all_tasks_valid and all_comps_valid)
    def confirmar_ot(self):
        def sí(_):
            popup.dismiss()
            self._do_save_ot()

        def no(_):
            popup.dismiss()

        content = BoxLayout(orientation='vertical', padding=dp(10), spacing=dp(10))
        lbl = Label(text="¿Confirmar nueva Orden de Trabajo?", halign='center')
        lbl.bind(size=lambda i, s: setattr(i, 'text_size', i.size))
        btns = BoxLayout(spacing=dp(10), size_hint_y=None, height=dp(40))
        b1 = Button(text="Sí"); b2 = Button(text="No")
        b1.bind(on_release=sí); b2.bind(on_release=no)
        btns.add_widget(b1); btns.add_widget(b2)
        content.add_widget(lbl); content.add_widget(btns)

        popup = Popup(
            title="Confirmar OT",
            content=content,
            size_hint=(0.8, None),
            height=dp(180),
            auto_dismiss=False
        )
        popup.open()
    def _do_save_ot(self):
        # 1) tareas
        ch_t   = list(reversed(self.ids.tasks_grid.children))
        tasks  = []
        for i in range(0, len(ch_t), 3):
            tf = ch_t[i+1]
            tarea_id = next((tid for tid, txt in self.tasks_data if txt == tf.text), None)
            tasks.append(tarea_id)

        # 2)  componentes
        ch_c     = list(reversed(self.ids.comps_grid.children))
        comp_map = {}
        for i in range(0, len(ch_c), 3):
            spinner = ch_c[i]
            tf      = ch_c[i+1]
            fila    = int(spinner.text)
            cid     = next((cid for cid, txt in self.comps_data if txt == tf.text), None) if tf.text else None
            comp_map[fila] = cid

        # 3) Construir motivo (cadena de texto)
        motivo = "\n".join(
            f"{tasks[i] or ''}:{comp_map.get(i+1) or ''}"
            for i in range(len(tasks))
        )

        # 4) Obtener IDs desde el screen de Preventivos
        prev       = self.manager.get_screen('preventivos')
        resp       = prev.id_responsable
        planmto_id = prev.ID_OT_Planmto

        # 5) Recuperar el ID_Equipo_Estructura desde la tabla ot_planmto
        conn = obtener_conexion_sqlserver(); cur = conn.cursor()
        cur.execute(
            "SELECT ID_Equipo_Estructura FROM ot_planmto WHERE ID_OT_Planmto = ?",
            [planmto_id]
        )
        row = cur.fetchone()
        if not row:
            conn.close()
            Snackbar(text="No encontré el equipo asociado al preventivo").open()
            return
        eq_struct = row[0]

        # 6) Validar duplicado
        cur.execute("""
            SELECT COUNT(*) FROM ot
            WHERE ID_Responsable=? AND ID_Equipo_Estructura=? AND Motivo=?
        """, [resp, eq_struct, motivo])
        if cur.fetchone()[0] > 0:
            conn.close()
            Snackbar(text="Ya existe una OT con estos datos").open()
            return

        # 7) Insertar la nueva OT
        nueva_ot = self._gen_simple_ot()
        ahora    = datetime.now()
        cur.execute("""
            INSERT INTO ot
            (ID_Responsable, ID_Equipo_Estructura,
            Motivo, OT, Fecha, ID_OT_Planmto,
            Hora_Parada, Observaciones, Habilitado)
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?, 'Si')
        """, [
            resp,
            eq_struct,
            motivo,
            nueva_ot,
            ahora,
            planmto_id,
            self.ids.obs_input.text
        ])
        conn.commit()
        conn.close()

        Popup(
            title="Éxito",
            content=Label(text=f"Nueva OT generada:\n{nueva_ot}"),
            size_hint=(None, None), size=(dp(300), dp(150)),
            auto_dismiss=True
        ).open()
    def _gen_simple_ot(self):
        año = datetime.now().year
        año2 = str(año)[-2:]
        conn = obtener_conexion_sqlserver(); cur = conn.cursor()
        cur.execute("""
            SELECT TOP 1 OT 
            FROM ot
            WHERE YEAR(Fecha)=?
            ORDER BY Fecha DESC
        """, [año])
        row = cur.fetchone()
        n = 1
        if row:
            prev = row[0].split('-')[-1].split('/')[0]
            try: n = int(prev) + 1
            except: n = 1
        conn.close()
        return f"OT-{n}/{año2}"

class RegistrarTareaScreen(Screen): #Ventana de Registrar una tarea dentro de Preventivos
    MAX_FILAS = 8

    def on_pre_enter(self):
        # — Carga de datos
        raw_t = obtener_tareas()
        self.tasks_data = [(r.ID_Tarea, r.Tarea) for r in raw_t]
        self.task_texts = [t for _, t in self.tasks_data]

        raw_c = obtener_componentes()
        self.comps_data = [(r[0], r[1]) for r in raw_c]
        self.comp_texts = [t for _, t in self.comps_data]

        # — Reset estado
        self._n_tasks = 0
        self._n_comps  = 0
        self.ids.tasks_grid.clear_widgets()
        self.ids.comps_grid.clear_widgets()
        self.ids.btn_registrar.disabled = True

        # — Agregar campo Observaciones
        self.ids.obs_input.text = ''
        self.ids.obs_count.text = '0/300'

        # — Construir date/time pickers
        ahora = datetime.now()
        date_layout = self.ids.date_layout
        date_layout.clear_widgets()
        col1, self.date_inicio, self.hora_inicio, self.msg1 = \
            FechaHoraBackend.build_fecha_hora(
                ahora.strftime("%d/%m/%Y"),
                ahora.strftime("%H:%M"),
                "Inicio",
                lambda ti: FechaHoraBackend.abrir_datepicker(
                    ti, self.date_inicio, self.hora_inicio,
                    self.date_fin,   self.hora_fin,
                    self.validar_fechas
                ),
                self.validar_fechas
            )
        col2, self.date_fin,   self.hora_fin,   self.msg2 = \
            FechaHoraBackend.build_fecha_hora(
                ahora.strftime("%d/%m/%Y"),
                ahora.strftime("%H:%M"),
                "Fin",
                lambda ti: FechaHoraBackend.abrir_datepicker(
                    ti, self.date_inicio, self.hora_inicio,
                    self.date_fin,   self.hora_fin,
                    self.validar_fechas
                ),
                self.validar_fechas
            )
        date_layout.add_widget(col1)
        date_layout.add_widget(col2)

        # — Asegurar validación al cambiar hora
        self.hora_inicio.bind(text=lambda *a: self.validar_fechas())
        self.hora_fin   .bind(text=lambda *a: self.validar_fechas())
    def volver(self):
        self.manager.current = 'preventivos'
        if hasattr(self, 'parent_popup'):
            Clock.schedule_once(lambda dt: self.parent_popup.open(), 0)
    def add_task_row(self):#Añadir fila de tarea
        if self._n_tasks >= self.MAX_FILAS:
            return
        self._n_tasks += 1
        grid = self.ids.tasks_grid

        lbl = Label(
            text=str(self._n_tasks),
            size_hint_x=0.1, size_hint_y=None, height=dp(35),
            halign='center', valign='middle'
        )
        lbl.bind(size=lambda inst, sz: setattr(inst, 'text_size', inst.size))

        tf = MDTextField(
            hint_text='Seleccione…',
            size_hint_x=0.8, size_hint_y=None, height=dp(35),
            multiline=False
        )
        tf.selected = False
        tf.menu = MDDropdownMenu(caller=tf, items=[], width_mult=4, max_height=dp(200))
        tf.bind(on_focus=lambda inst, f: inst.menu.open() if f and inst.menu.items else inst.menu.dismiss())
        tf.bind(text=self._on_task_text)
        tf.bind(on_focus=self._validate_task_exact)

        btn = Button(
            text='X',
            size_hint_x=0.1, size_hint_y=None, height=dp(35),
            
        )
        btn.bind(height=lambda inst, h: setattr(inst, 'font_size', h * 0.6))
        btn.font_size = btn.height * 0.6
        btn.bind(on_release=lambda *_: self._remove_task_row(lbl, tf, btn))

        grid.add_widget(lbl)
        grid.add_widget(tf)
        grid.add_widget(btn)
        self._update_registrar_btn()

        Clock.schedule_once(lambda dt: setattr(btn,     'font_size', btn.height     * 0.6), 0)
       
        for w in self.ids.comps_grid.children:
            if isinstance(w, Spinner):
                w.values = [str(i) for i in range(1, self._n_tasks + 1)]
                if int(w.text) > self._n_tasks:
                    w.text = str(self._n_tasks) 
    def _on_task_text(self, tf, texto):
        tf.selected = False
        filt = texto.lower()
        opciones = [t for (_id, t) in self.tasks_data if filt in t.lower()]
        menu = tf.menu
        menu.dismiss()
        if opciones:
            menu.items = [{
                'viewclass': 'OneLineListItem',
                'text': item,
                'height': dp(35),
                'on_release': lambda x=item: self._select_task(x, tf)
            } for item in opciones]
            menu.open()
        self._update_registrar_btn()
    def _validate_task_exact(self, tf, focus):
        if not focus and not tf.selected:
            tf.text = ''
            Snackbar(text="Seleccione una tarea válida").open()
        self._update_registrar_btn()
    def _select_task(self, texto, tf):
        tf.text = texto
        tf.selected = True
        tf.menu.dismiss()
        self._update_registrar_btn()
    def _remove_task_row(self, lbl, tf, btn):
        grid = self.ids.tasks_grid
        for w in (lbl, tf, btn):
            grid.remove_widget(w)
        self._n_tasks -= 1

        # Renumerar solo Labels de numeración (excluyendo botones “X”)
        count = 1
        for child in grid.children[::-1]:
            if isinstance(child, Label) and not isinstance(child, Button):
                child.text = str(count)
                count += 1

        # REFRESCAR los valores de todos los Spinners en comps_grid
        for w in self.ids.comps_grid.children:
            if isinstance(w, Spinner):
                w.values = [str(i) for i in range(1, self._n_tasks + 1)]
                # si el valor actual ya no existe, lo reseteamos
                if int(w.text) > self._n_tasks:
                    w.text = str(self._n_tasks) 

        self._update_registrar_btn()
    def add_comp_row(self):#Añadir fila de componente
        if self._n_comps >= self.MAX_FILAS:
            return
        self._n_comps += 1
        grid = self.ids.comps_grid

        spinner = Spinner(
            text='1',
            values=[str(i) for i in range(1, self._n_tasks + 1)],
            size_hint_x=0.1, size_hint_y=None, height=dp(35),
            
        )
        spinner.bind(height=lambda inst, h: setattr(inst, 'font_size', h * 0.6))
        spinner.font_size = spinner.height * 0.6
        spinner.bind(text=lambda *a: self._update_registrar_btn())

        tf = MDTextField(
            hint_text='Seleccione…',
            size_hint_x=0.5, size_hint_y=None, height=dp(35),
            multiline=False
        )
        tf.selected = False
        tf.menu = MDDropdownMenu(caller=tf, items=[], width_mult=4, max_height=dp(200))
        tf.bind(on_focus=lambda inst, f: inst.menu.open() if f and inst.menu.items else inst.menu.dismiss())
        tf.bind(text=self._on_comp_text)
        tf.bind(on_focus=self._validate_comp_exact)

        ti = TextInput(
            hint_text='Cant.',
            size_hint_x=0.2, size_hint_y=None, height=dp(35),
            multiline=False, halign='center'
        )
        ti.bind(text=self._on_quantity_text)

        btn = Button(
            text='X',
            size_hint_x=0.1, size_hint_y=None, height=dp(35),
            
        )
        btn.bind(height=lambda inst, h: setattr(inst, 'font_size', h * 0.6))
        btn.font_size = btn.height * 0.6
        btn.bind(on_release=lambda *_: self._remove_comp_row(spinner, tf, ti, btn))

        for w in (spinner, tf, ti, btn):
            grid.add_widget(w)
        self._update_registrar_btn()

        Clock.schedule_once(lambda dt: setattr(spinner, 'font_size', spinner.height * 0.6), 0)
        Clock.schedule_once(lambda dt: setattr(btn,     'font_size', btn.height     * 0.6), 0)
    def _on_comp_text(self, tf, texto):
        tf.selected = False
        filt = texto.lower()
        opciones = [c for (_id, c) in self.comps_data if filt in c.lower()]
        menu = tf.menu
        menu.dismiss()
        if opciones:
            menu.items = [{
                'viewclass': 'OneLineListItem',
                'text': item,
                'height': dp(35),
                'on_release': lambda x=item: self._select_comp(x, tf)
            } for item in opciones]
            menu.open()
        self._update_registrar_btn()
    def _validate_comp_exact(self, tf, focus):
        if not focus and not tf.selected:
            tf.text = ''
            Snackbar(text="Seleccione un componente válido").open()
        self._update_registrar_btn()
    def _select_comp(self, texto, tf):
        tf.text = texto
        tf.selected = True
        tf.menu.dismiss()
        self._update_registrar_btn()
    def _on_quantity_text(self, ti, texto):
        clean = ''.join(c for c in texto.replace(',', '.') if c.isdigit() or c == '.')
        if clean != texto:
            Snackbar(text="Solo números y punto permitido").open()
        ti.text = clean
        self._update_registrar_btn()
    def _remove_comp_row(self, spinner, tf, ti, btn):
        grid = self.ids.comps_grid
        for w in (spinner, tf, ti, btn):
            grid.remove_widget(w)
        self._n_comps -= 1
        self._update_registrar_btn()
    def on_obs_text(self, inst, txt):#Observaciones
        if len(txt) > 300:
            inst.text = txt[:300]
            txt = inst.text
        self.ids.obs_count.text = f"{len(txt)}/300"
        self._update_registrar_btn()
    def validar_fechas(self):#Validación de fechas
        try:
            f1 = datetime.strptime(self.date_inicio.text, "%d/%m/%Y")
            f1 = f1.replace(
                hour=int(self.hora_inicio.text[:2]),
                minute=int(self.hora_inicio.text[3:])
            )
            f2 = datetime.strptime(self.date_fin.text, "%d/%m/%Y")
            f2 = f2.replace(
                hour=int(self.hora_fin.text[:2]),
                minute=int(self.hora_fin.text[3:])
            )
            ahora_local = datetime.now()

            # Mostrar Snackbar en cada error
            if f1 > f2:
                Snackbar(text="La fecha/hora de inicio es mayor que la de fin").open()
            if f2 > ahora_local:
                Snackbar(text="La fecha/hora de fin es mayor que la actual").open()

            # Mensajes internos (vacío si OK)
            self.msg1.text = "" if f1 <= f2 else "Inicio > Fin"
            self.msg2.text = "" if f2 <= ahora_local else "Fin > ahora"

            # Botón según mensajes
            self.ids.btn_registrar.disabled = not (
                self.msg1.text == "" and self.msg2.text == ""
            )
        except Exception:
            self.ids.btn_registrar.disabled = True
    def _update_registrar_btn(self):#Habilitar botón Registrar
        # Tareas válidas
        task_fields = [
            w for w in self.ids.tasks_grid.children
            if isinstance(w, MDTextField)
        ]
        tasks_ok = bool(task_fields) and all(
            getattr(w, 'selected', False) for w in task_fields
        )

        #  Componentes: extraigo todos los spinners y sus textos
        spinners = [
            w for w in self.ids.comps_grid.children
            if isinstance(w, Spinner)
        ]
        textos = [s.text for s in spinners if s.text]

        #  Si hay duplicados en 'textos', falla
        if len(textos) != len(set(textos)):
            comps_ok = False
        else:
            # compruebo que cada componente tenga selección válida y cantidad
            comps = list(self.ids.comps_grid.children)
            comps_ok = True
            for i in range(0, len(comps), 4):
                tf = comps[i+1]   # MDTextField componente
                ti = comps[i+2]   # TextInput cantidad
                # selected en tf y ti.text no vacío
                if not (getattr(tf, 'selected', False) and ti.text):
                    comps_ok = False
                    break

        # 3) Fechas
        fechas_ok = (not self.msg1.text) and (not self.msg2.text)

        # 4) Aplico al botón
        self.ids.btn_registrar.disabled = not (tasks_ok and comps_ok and fechas_ok)
    def registrar(self):#Confirmación y guardado en BD
        def sí(_):
            popup.dismiss()
            self._do_save_registro()
        def no(_):
            popup.dismiss()

        content = BoxLayout(orientation='vertical', padding=dp(10), spacing=dp(10))
        content.add_widget(Label(text="Confirmar registro de tarea?", halign='center'))
        btns = BoxLayout(spacing=dp(10), size_hint_y=None, height=dp(40))
        for txt, fn in [("Sí", sí), ("No", no)]:
            b = Button(text=txt); b.bind(on_release=fn); btns.add_widget(b)
        content.add_widget(btns)

        popup = Popup(
            title="Registrar Tarea",
            content=content,
            size_hint=(0.8, None),
            height=dp(180),
            auto_dismiss=False
        )
        popup.open()
    def _gen_simple_ot(self):
        año  = datetime.now().year
        año2 = str(año)[-2:]
        conn = obtener_conexion_sqlserver(); cur = conn.cursor()
        cur.execute("""
            SELECT TOP 1 OT
              FROM ot
             WHERE YEAR(Fecha)=?
             ORDER BY Fecha DESC
        """, [año])
        row = cur.fetchone()
        n = 1
        if row:
            prev = row[0].split('-')[-1].split('/')[0]
            try:
                n = int(prev) + 1
            except:
                n = 1
        conn.close()
        return f"OT-{n}/{año2}"
    def _do_save_registro(self):
        """
        1) Construye motivo “ID_Tarea:ID_Componente” por línea.
        2) Inserta en ot con Habilitado='No', captura ID_OT (OUTPUT INSERTED).
        3) Inserta en otmto, captura ID_OTMTO (OUTPUT INSERTED).
        4) Inserta en stock por cada componente.
        """
        
        # Recopila tareas en orden
        ch_t = list(reversed(self.ids.tasks_grid.children))
        tasks = []
        for i in range(0, len(ch_t), 3):
            tf = ch_t[i+1]
            tid = next((tid for tid, txt in self.tasks_data if txt == tf.text), None)
            tasks.append(tid)

        # 2) Recopila componentes y cantidades
        ch_c = list(reversed(self.ids.comps_grid.children))
        comp_map = {}   # fila -> ID_Componente
        comp_qty = {}   # fila -> cantidad
        for i in range(0, len(ch_c), 4):
            spinner = ch_c[i]
            tf_sp    = ch_c[i+1]
            ti_qty   = ch_c[i+2]
            fila     = int(spinner.text)
            cid      = next((cid for cid, txt in self.comps_data if txt == tf_sp.text), None)
            comp_map[fila] = cid
            comp_qty[fila] = float(ti_qty.text)
        
        # Construir motivo
        motivo = "\n".join(
            f"{tasks[i] or ''}:{comp_map.get(i+1) or ''}"
            for i in range(len(tasks))
        )
      
        # Datos de contexto
        resp = self.manager.get_screen('preventivos').id_responsable
        eq   = self.manager.get_screen('preventivos').ID_OT_Planmto
        obs  = self.ids.obs_input.text

        # Conexión y chequeo de duplicados en OT
        conn = obtener_conexion_sqlserver()
        cur  = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM ot "
            "WHERE ID_Responsable=? AND ID_Equipo_Estructura=? AND Motivo=?",
            [resp, eq, motivo]
        )
        if cur.fetchone()[0] > 0:
            conn.close()
            Snackbar(text="Ya existe una OT con estos datos").open()
            return

        # Insertar en ot y capturar ID_OT
        ahora    = datetime.now()
        nueva_ot = self._gen_simple_ot()
        cur.execute("""
            INSERT INTO ot
              (ID_Responsable, ID_Equipo_Estructura, Motivo, OT, Fecha,
               ID_OT_Planmto, Hora_Parada, Observaciones, Habilitado)
            OUTPUT INSERTED.ID_OT
            VALUES (?,?,?,?,?,? ,NULL,?, 'No')
        """, [resp, eq, motivo, nueva_ot, ahora, eq, obs])
        id_ot = cur.fetchone()[0]

        # Obtener ID_Establecimiento
        cur.execute(
            "SELECT ID_Establecimiento "
            "FROM relacion_establecimiento_usuario "
            "WHERE ID_Responsable = ?",
            [resp]
        )
        row = cur.fetchone()
        id_est = row[0] if row else None

        #Insertar en otmto y capturar ID_OTMTO
        cod = f"MTO-{nueva_ot}"
        # parsear fecha/hora de inicio y fin
        f1 = datetime.strptime(self.date_inicio.text, "%d/%m/%Y")
        h1 = datetime.strptime(self.hora_inicio.text, "%H:%M").time()
        fi = datetime.combine(f1.date(), h1)
        f2 = datetime.strptime(self.date_fin.text, "%d/%m/%Y")
        h2 = datetime.strptime(self.hora_fin.text, "%H:%M").time()
        ff = datetime.combine(f2.date(), h2)

        cur.execute("""
            INSERT INTO otmto
              (ID_OT, ID_OT_Planmto, ID_Responsable, Fecha, Tarea_realizada,
               Cod_OTMTO, Estado, Observaciones, Hora_Inicio, Hora_Fin, Habilitado)
            OUTPUT INSERTED.ID_OTMTO
            VALUES (?,?,?,?,?,? ,?,?,?,?,'No')
        """, [id_ot, eq, resp, ahora, motivo, cod, 100.00, obs, fi, ff])
        id_otmto = cur.fetchone()[0]

        #Insertar en stock de componentes
        for fila, cid in comp_map.items():
            cantidad = comp_qty[fila]
            cur.execute("""
                INSERT INTO stock
                  (Fecha_movimiento, Movimiento, ID_Responsable,
                   ID_Establecimiento, ID_OTMTO, ID_Componente, Cantidad)
                VALUES (?,?,?,?,?,?,?)
            """, [ahora, 'S', resp, id_est, id_otmto, cid, cantidad])

   
        # Commit, cerrar y notificar
        conn.commit()
        conn.close()
        Snackbar(text="Registro completado").open()
        self.volver()

class RowContainer(GridLayout):#Ecualizador de alturas de filas en OT
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # cada vez que se añadan/remuevan Filas, redimensiona
        self.bind(children=lambda *a: Clock.schedule_once(self._equalize_height, 0))

    def _equalize_height(self, dt):
        # quien tenga la celda más alta...
        heights = [child.height for child in self.children if hasattr(child, 'height')]
        if not heights:
            return
        max_h = max(heights)
        # y la aplicamos a todos
        for child in self.children:
            if hasattr(child, 'height'):
                child.height = max_h
        # y a nosotros mismos
        self.height = max_h



class DetalleOTPopup(Popup):
    def on_obs_text(self, new_text: str): #Recuadro de observaciones
        # 1) recortar a 300 si es necesario
        if len(new_text) > 300:
            trimmed = new_text[:300]
            ti = self.ids.obs_input
            ti.unbind(text=self.on_obs_text)
            ti.text = trimmed
            ti.bind(text=self.on_obs_text)
            new_text = trimmed

        # 2) actualizar contador en el label interno del popup
        count = len(new_text)
        self.ids.obs_label.text = f"Observaciones ({count}/300)"





class OrdenesTrabajoScreen(Screen):
    
    name = 'ordenes_trabajo'

    

    def set_user_data(self, usuario, responsable, establecimiento, id_establecimiento, id_responsable):
        self.usuario = usuario
        self.responsable = responsable
        self.establecimiento = establecimiento
        self.id_establecimiento = id_establecimiento
        self.id_responsable = id_responsable  
           
        
    def abrir(self):
        """Se llama desde MantenimientoScreen para mostrar esta pantalla."""
        self.manager.current = self.name
        self.cargar_datos_ot_pendientes()

    def volver(self):
        """Regresa a la pantalla de Mantenimiento."""
        self.manager.current = 'mantenimiento'

    def cargar_datos_ot_pendientes(self):
        grid = self.ids.ot_grid
        grid.clear_widgets()

        # Encabezados
        for title in ('Fecha','OT','Sector','Equipo-Estructura','Finalizar'):
            grid.add_widget(Factory.DataCell(text=title))

        conn = obtener_conexion_sqlserver()
        cur  = conn.cursor()

        # <-- REEMPLAZA ESTE BLOQUE -->
        sql = """
            SELECT DISTINCT
                o.ID_OT,
                o.OT,
                o.Fecha,
                o.ID_Equipo_Estructura
            FROM ot AS o
            -- 1) unimos primero equipo->sector
            JOIN relacion_sector_equipo_estructura AS rse
            ON rse.ID_Equipo_Estructura = o.ID_Equipo_Estructura
            -- 2) luego sector->establecimiento, filtrando por el id del usuario
            JOIN relacion_establecimiento_sector AS res
            ON res.ID_Sector = rse.ID_Sector
            AND res.ID_Establecimiento = ?
            WHERE o.Habilitado = 'Si'
            ORDER BY o.Fecha DESC
        """
        cur.execute(sql, [self.id_establecimiento])
        rows = cur.fetchall()
        # <-- HASTA AQUÍ -->

        for id_ot, ot_num, fecha_dt, id_eq in rows:
            fecha_str = fecha_dt.strftime("%d/%m/%y")

            # Sector (ya filtrado) y nombre de equipo
            cur.execute("""
                SELECT s.Sector
                FROM relacion_sector_equipo_estructura re
                JOIN sectores s ON s.ID_Sector = re.ID_Sector
                WHERE re.ID_Equipo_Estructura = ?
            """, [id_eq])
            sector = (cur.fetchone() or [""])[0]

            cur.execute(
                "SELECT Equipo_Estructura FROM equipo_estructura WHERE ID_Equipo_Estructura = ?",
                [id_eq]
            )
            equipo = (cur.fetchone() or [""])[0]

            grid.add_widget(Factory.DataCell(text=fecha_str))
            grid.add_widget(Factory.DataCell(text=ot_num))
            grid.add_widget(Factory.DataCell(text=sector))
            grid.add_widget(Factory.DataCell(text=equipo))

            btn = Factory.DataButton(text='Ir')
            btn.bind(on_release=lambda _, pk=id_ot: self.abrir_detalle_ot(pk))
            grid.add_widget(btn)

        cur.close()
        conn.close()



    def obtener_avance(id_ot):#Recargar los avances guardados en tabla otmto
        conexion = obtener_conexion_sqlserver()
        cursor = conexion.cursor()
        consulta = """
            SELECT Estado, Hora_Inicio, Hora_Fin, Observaciones, Tarea_realizada
            FROM otmto
            WHERE ID_OT = ?
        """
        try:
            cursor.execute(consulta, [id_ot])
            registro = cursor.fetchone()
        except Exception as e:
            print(f"Error obteniendo avance: {e}")
            registro = None
        conexion.close()
        return registro
    
    def obtener_componentes_por_equipo(self, id_equipo):
        """Devuelve lista de dicts {'ID':…, 'Componente':…} para ese equipo,
        y la imprime en consola para depuración."""
        conn = obtener_conexion_sqlserver()
        cur = conn.cursor()
        cur.execute("""
            SELECT c.ID_Componente, c.Componente
            FROM componentes c
            JOIN relacion_equipo_estructura_componentes rc
            ON c.ID_Componente = rc.ID_Componente
            WHERE rc.ID_Equipo_Estructura = ?
            AND c.Habilitado = 'Si'
        """, [id_equipo])
        rows = cur.fetchall()
        conn.close()

        # Creamos la lista de salida
        components = [(r[0], r[1]) for r in rows]

        # Imprimimos en consola para verificar qué recuperamos
        print(f"Componentes obtenidos para el equipo {id_equipo}: {components}")

        return components
   
    def obtener_precargados_componentes_ot(self):
        """Trae de la BD los componentes ya guardados en ot.Motivo (si hay)."""
        if hasattr(self, 'componentes_provisionales_ot') and self.componentes_provisionales_ot is not None:
            return self.componentes_provisionales_ot
        # suponemos que tienes una función similar que lee de otmto o de ot:
        self.componentes_provisionales_ot = obtener_precargados_componentes_db_ot(self.id_ot)
        return self.componentes_provisionales_ot

    def abrir_modal_componentes_ot(self, components):
        """Abre el mismo ComponentesModal que usas en DetalleTareas."""
        #if not comp_options:
            #return
        precargados = self.obtener_precargados_componentes_ot()
        modal = ComponentesModal(components, precargados)
        modal.open()
        # al cerrar, guarda y recarga el detalle:
        modal.bind(on_dismiss=lambda inst: self.actualizar_componentes_provisionales_ot(modal.obtener_datos()))

    def actualizar_componentes_provisionales_ot(self, nuevos_componentes):
        self.componentes_provisionales_ot = nuevos_componentes

    def _guardar_componentes_ot(self, datos):
        """
        Datos = [{'id': ID_Componente, 'cantidad': …}, …]
        Construimos la cadena que irá en otmto.Tarea_realizada, un ID por línea.
        """
        lineas = [str(d['id']) for d in datos]
        nueva_cadena = "\n".join(lineas)

        conn = obtener_conexion_sqlserver()
        cur = conn.cursor()
        cur.execute("UPDATE otmto SET Tarea_realizada = ? WHERE ID_OT = ?", [nueva_cadena, self.id_ot])
        conn.commit()
        conn.close()

        # refresca el popup:
        self.abrir_detalle_ot(self.id_ot)

    def abrir_detalle_ot(self, id_ot):
        popup = Factory.DetalleOTPopup()
        self.id_ot = id_ot
        


        # 1) Conexión y consulta OT + nombre de Equipo
        conn = obtener_conexion_sqlserver()
        cur = conn.cursor()
        cur.execute("""
            SELECT o.ID_Equipo_Estructura,
                   eq.Equipo_Estructura,
                   o.OT,
                   o.Observaciones,
                   o.Hora_parada,
                   o.Motivo
            FROM ot o
            LEFT JOIN equipo_estructura eq
              ON o.ID_Equipo_Estructura = eq.ID_Equipo_Estructura
            WHERE o.ID_OT = ?
        """, [id_ot])
        fila = cur.fetchone()
        if not fila:
            cur.close(); conn.close()
            return

        id_equipo, equipo_nombre, ot_num, observ, hora_parada, motivos_raw = fila

        # 2) Rellenar encabezado
        popup.ids.lbl_equipo.text = equipo_nombre or ""
        popup.ids.lbl_ot.text     = str(ot_num)

        # 3) Observaciones y hora parada
        popup.ids.obs_ot_label.text  = observ or ""
        popup.ids.hora_ot_label.text = hora_parada.strftime("%H:%M") if hora_parada else ""

        # 4) Tabla de motivos + CheckBox
        grid = popup.ids.rows_grid
        grid.clear_widgets()
        self.checkbox_list = []
        self.checkbox_info = []

        # … dentro de abrir_detalle_ot, después de limpiar el grid …
        
        if motivos_raw:
            for linea in motivos_raw.strip().splitlines():
                # línea: "ID_Tarea:ID_Componente" o "ID_Tarea:"
                if ":" not in linea:
                    continue
                tid_str, cid_str = linea.split(":", 1)
                # 1) ID_Tarea siempre debe ser entero
                try:
                    tid = int(tid_str)
                except ValueError:
                    continue
                # 2) ID_Componente opcional
                try:
                    cid = int(cid_str)
                except ValueError:
                    cid = None

                # 3) Obtener descripción de tarea
                cur.execute("SELECT Tarea FROM tareas WHERE ID_Tarea = ?", [tid])
                tarea_desc = (cur.fetchone() or [f"Tarea #{tid}"])[0]

                # 4) Si hay componente, obtener su descripción
                if cid is not None:
                    cur.execute("SELECT Componente FROM componentes WHERE ID_Componente = ?", [cid])
                    comp_desc = (cur.fetchone() or [f"Componente #{cid}"])[0]
                    texto = f"{tarea_desc}: {comp_desc}"
                else:
                    texto = tarea_desc

                # 5) Crear widgets y añadirlos al grid
                cell = Factory.DataCell(text=texto)
                chk  = CheckBox(size_hint=(None, None), size=(30, 30))
                container = Factory.CheckCell()
                container.add_widget(chk)

                grid.add_widget(cell)
                grid.add_widget(container)
                self.checkbox_list.append(chk)
                self.checkbox_info.append((chk, tid, cid))

        else:
            # Si no hay motivos, dejamos una fila vacía
            cell = Factory.DataCell(text="(sin motivos)")
            chk  = CheckBox(size_hint=(None, None), size=(30, 30))
            container = Factory.CheckCell()
            container.add_widget(chk)
            grid.add_widget(cell)
            grid.add_widget(container)
            self.checkbox_list.append(chk)
            self.checkbox_info.append(chk,None,None)

        # 5) Porcentaje de avance
        cur.execute("SELECT Estado FROM otmto WHERE ID_OT = ?", [id_ot])
        av = cur.fetchone()
        popup.ids.progress_bar.value = float(av[0]) if av else 0

        # 6) Selectores Fecha/Hora
        dl = popup.ids.date_layout
        dl.clear_widgets()
        hoy = datetime.now()
        f_act = hoy.strftime("%d/%m/%Y")
        h_act = hoy.strftime("%H:%M")

       
        def update_progress(popup):
            total = len(self.checkbox_list)
            done  = sum(1 for c in self.checkbox_list if c.active)
            popup.ids.progress_bar.value = (done / total) * 100 if total else 0

        # ligar cada casilla al recálculo
        for chk in self.checkbox_list:
            chk.bind(active=lambda inst, val, pop=popup, fn=update_progress: fn(pop))

        def validar_fechas():
            try:
                f1 = datetime.strptime(date_inicio.text, "%d/%m/%Y")
                f1 = f1.replace(hour=int(hora_inicio.text[:2]), minute=int(hora_inicio.text[3:]))
                f2 = datetime.strptime(date_fin.text, "%d/%m/%Y")
                f2 = f2.replace(hour=int(hora_fin.text[:2]), minute=int(hora_fin.text[3:]))
                now = datetime.now()
                msg1.text = "" if f1 <= f2 else "Inicio > Fin"
                msg2.text = "" if f2 <= now else "Fin > Ahora"
                btn_guardar.disabled = not (msg1.text == "" and msg2.text == "")
            except:
                btn_guardar.disabled = True

        def abrir_dp(inp):
            abrir_datepicker(inp, date_inicio, hora_inicio, date_fin, hora_fin, validar_fechas)

        col1, date_inicio, hora_inicio, msg1 = build_fecha_hora(
            f_act, h_act, "Inicio", abrir_dp, validar_fechas
        )
        col2, date_fin,    hora_fin,    msg2 = build_fecha_hora(
            f_act, h_act, "Fin",    abrir_dp, validar_fechas
        )
        dl.add_widget(col1)
        dl.add_widget(col2)

        self.popup = popup

        # 7) Botones y bindings
        btn_volver    = popup.ids.btn_volver
        btn_guardar   = popup.ids.btn_guardar
        btn_finalizar_ot = popup.ids.btn_finalizar_ot
        
        #  boton Añadir componentes
        comp_options = self.obtener_componentes_por_equipo(id_equipo) # usa el id_equipo cargado.
        btn_add = popup.ids.btn_añadir_comp
        btn_add.bind(on_release=lambda instance: self.abrir_modal_componentes_ot(comp_options))

        btn_volver.bind(on_release=lambda *_: popup.dismiss())
        btn_guardar.bind(on_release=lambda *_: self.do_guardar_otmto(
            date_inicio, hora_inicio, date_fin, hora_fin, msg1, msg2, self.checkbox_list
        ))
        btn_finalizar_ot.bind(on_release=lambda *_: self.finalizar_ot())
        
        
        # 8) Precarga si ya hay avance guardado
        registro = obtener_avance_otmto(self.id_ot)
        
        if registro:
            estado, hi, hf, obs_guard, tareas_guard = registro
            popup.ids.progress_bar.value = float(estado)
            date_inicio.text = hi.strftime("%d/%m/%Y")
            hora_inicio.text = hi.strftime("%H:%M")
            date_fin.text    = hf.strftime("%d/%m/%Y")
            hora_fin.text    = hf.strftime("%H:%M")
            if obs_guard:
                popup.ids.obs_input.text = obs_guard
            lines = tareas_guard.splitlines()
            for chk, tid, cid in self.checkbox_info:
                key = f"{tid}:{cid or ''}"
                if key in lines:
                    chk.active = True
        
        # 9) Validación dinámica
        for w in (date_inicio, hora_inicio, date_fin, hora_fin):
            w.bind(text=lambda *a: validar_fechas())
        for chk in self.checkbox_list:
            chk.bind(active=lambda *a: validar_fechas())

        

        # 10) Mostrar popup y cerrar conexión
        popup.open()
        cur.close()
        conn.close()

    def do_guardar_otmto(self, date_inicio, hora_inicio, date_fin, hora_fin, msg1, msg2, checkbox_list):
        # 1) Parseo de fechas y horas a datetime
        try:
            f1 = datetime.strptime(date_inicio.text, "%d/%m/%Y")
            f1 = f1.replace(hour=int(hora_inicio.text[:2]), minute=int(hora_inicio.text[3:]))
            f2 = datetime.strptime(date_fin.text, "%d/%m/%Y")
            f2 = f2.replace(hour=int(hora_fin.text[:2]), minute=int(hora_fin.text[3:]))
        except Exception as ex:
            print("Error al parsear fechas:", ex)
            return

        # 2) Cadena de tareas activas
        seleccion = [
            (tid, cid) for chk, tid, cid in self.checkbox_info
            if chk.active
        ]
        tareas_concatenadas = "\n".join(
            f"{tid}:{cid or ''}" for tid, cid in seleccion
        )

        # 3) Datos de OT
        ot_text = self.popup.ids.lbl_ot.text
        estado  = self.popup.ids.progress_bar.value
        observ  = self.popup.ids.obs_input.text

        # 4) Valores extra que necesitas pasar
        id_ot_planmto  = getattr(self, 'id_ot_planmto', None)
        #id_responsable = getattr(self, 'id_responsable', None)
        ot_codigo      = f"MTO - {ot_text}"

        # 5) Insertar o actualizar usando f1/f2 en lugar de atributos
        if obtener_avance_otmto(self.id_ot):
            actualizar_avance_otmto(
                self.id_ot,
                ot_codigo,
                estado,
                f1,
                f2,
                observ,
                tareas_concatenadas
            )
        else:
            insertar_avance_otmto(
                self.id_ot,
                id_ot_planmto,
                self.id_responsable,
                ot_codigo,
                estado,
                f1,
                f2,
                observ,
                tareas_concatenadas
            )
        if self.componentes_provisionales_ot:
            fecha_mov = datetime.now()
            actualizar_stock_componentes_ot(
                self.id_ot, 
                fecha_mov,
                self.id_establecimiento, 
                self.id_responsable,
                self.componentes_provisionales_ot
            )

        # 6) Cerrar popup
        #self.popup.dismiss()


    def guardar_avance(self, ot, estado, hora_inicio, hora_fin, observaciones, tareas_concatenadas):
        # Validaciones y llamadas a funciones para insertar/actualizar avances en la base de datos.
        if not (hora_inicio and hora_fin):
            Popup(title="Error", content=(Label(text="Debe seleccionar Hora de Inicio y de Fin.",size_hint_y=None, height=dp(60),halign='center', valign='middle',text_size=(Window.width * 0.8 - dp(20), None))),
                  size_hint=(None, None), size=(300,200)).open()
            return
        if hora_inicio >= hora_fin:
            Popup(title="Error", content=(Label(text="La Hora de Inicio debe ser menor que la Hora de Fin.",size_hint_y=None, height=dp(60),halign='center', valign='middle',text_size=(Window.width * 0.8 - dp(20), None))),
                  size_hint=(None, None), size=(300,200)).open()
            return
        if hora_fin > datetime.now():
            Popup(title="Error", content=(Label(text="La Hora de Fin debe ser menor o igual a la fecha y hora actual.",size_hint_y=None, height=dp(60),halign='center', valign='middle',text_size=(Window.width * 0.8 - dp(20), None))),
                  size_hint=(None, None), size=(300,200)).open()
            return
        if not tareas_concatenadas.strip():
            Popup(title="Error", content=(Label(text="Debe seleccionar al menos una tarea.",size_hint_y=None, height=dp(60),halign='center', valign='middle',text_size=(Window.width * 0.8 - dp(20), None))), 
                  size_hint=(None, None), size=(300,200)).open()
            return

        def on_confirm(instance):
            confirm_popup.dismiss()
            if obtener_avance_otmto(self.id_ot) is None:
                insertar_avance_otmto(self.id_ot, ot, estado, hora_inicio, hora_fin, observaciones, tareas_concatenadas)
            else:
                actualizar_avance_otmto(self.id_ot, ot, estado, hora_inicio, hora_fin, observaciones, tareas_concatenadas)
            if self.componentes_provisionales_ot:
                fecha_mov = datetime.now()
                actualizar_stock_componentes_ot(self.id_ot, fecha_mov, self.id_establecimiento, self.id_responsable, self.componentes_provisionales_ot)
            else:
                print("No se han seleccionado componentes; no se actualizará stock de componentes.")

        def on_cancel(instance):
            confirm_popup.dismiss()

        content_layout = BoxLayout(orientation='vertical', spacing=10, padding=dp(10))

        
        content_layout.add_widget(Label(text="¿Desea guardar el avance?",size_hint_y=None, height=dp(60),halign='center', valign='middle',text_size=(Window.width * 0.8 - dp(20), None)))
        btn_layout = BoxLayout(orientation='horizontal', size_hint_y=None, height=dp(40), spacing=dp(5))
        btn_si = Button(text="Sí", on_release=on_confirm)
        btn_no = Button(text="No", on_release=on_cancel)
        btn_si.bind(on_press=on_confirm)
        btn_no.bind(on_press=on_cancel)
        btn_layout.add_widget(btn_si)
        btn_layout.add_widget(btn_no)
        content_layout.add_widget(btn_layout)
        confirm_popup = Popup(title="Confirmación", content=content_layout, size_hint=(0.8, None), height=dp(180),auto_dismiss=False)
        confirm_popup.open()

    
    def finalizar_ot(self):
        
        def on_confirm(instance):
            # 1) Cerrar el popup de confirmación
            popup.dismiss()
            print("→ Finalizar Orden Trabajo: confirmación ACEPTADA")
            # Verificar que exista al menos un registro en otmto_planmto
            conn_check = obtener_conexion_sqlserver()
            cur_check = conn_check.cursor()
            cur_check.execute(
                "SELECT COUNT(*) FROM otmto WHERE ID_OT = ?",
                [self.id_ot]
            )
            if cur_check.fetchone()[0] == 0:
                conn_check.close()
                Popup(
                    title="Error",
                    content=Label(text="Debe guardar los cambios antes de finalizar la orden."),
                    size_hint=(0.8, None), height=dp(120)
                ).open()
                return
            conn_check.close()

            # —— 1) Deshabilitar el preventivo actual
            
            conn = obtener_conexion_sqlserver()
            cursor = conn.cursor()
            try:
                # 2) Deshabilitar el preventivo actual
                cursor.execute(
                    "UPDATE ot SET Habilitado = 'No' WHERE ID_OT = ?",
                    [self.id_ot]
                )
                conn.commit()
                print(f"    ✔ Orden de trabajo {self.id_ot} marcada como 'No' habilitada")

                

                #  Mostrar popup de éxito
                exitoso = Popup(
                    title="Registro finalizado con éxito",
                    content=Label(text=f"Se ha finalizado la orden de trabajo"),
                    size_hint=(0.8, None), height=dp(140),
                    auto_dismiss=True
                )
                exitoso.open()

                # 10) Cerrar el popup de detalles de tareas si existe
                if hasattr(self, 'popup') and self.popup:
                    self.popup.dismiss()

                # 11) Recargar y volver a la pantalla de preventivos
                prev = self.manager.get_screen('ordenes_trabajo')
                prev.cargar_datos_ot_pendientes()   
                self.manager.current = 'ordenes_trabajo'

            except Exception as e:
                Popup(
                    title="Error",
                    content=Label(text=str(e)),
                    size_hint=(0.8, None), height=dp(120)
                ).open()
            finally:
                conn.close()

        def on_cancel(instance):
            popup.dismiss()
            

        # Construcción del popup de confirmación
        content = BoxLayout(orientation='vertical', spacing=dp(10), padding=dp(10))
        content.add_widget(Label(
            text="¿Desea finalizar la orden de trabajo? Guarde los cambios antes de avanzar.",
            size_hint_y=None, height=dp(60),
            halign='center', valign='middle',
            text_size=(Window.width * 0.8 - dp(20), None)
        ))
        btns = BoxLayout(orientation='horizontal', size_hint_y=None, height=dp(40), spacing=dp(10))
        btn_si = Button(text="Sí", size_hint_x=0.5, on_release=on_confirm)
        btn_no = Button(text="No", size_hint_x=0.5, on_release=on_cancel)
        btns.add_widget(btn_si)
        btns.add_widget(btn_no)
        content.add_widget(btns)

        popup = Popup(
            title="Confirmación",
            content=content,
            size_hint=(0.8, None),
            height=dp(180),
            auto_dismiss=False
        )
        popup.open()

   

class MyApp(MDApp):
    title = "DS"
    
    
    def build(self):        
        # Se crea el ScreenManager y se agregan las pantallas (login y mantenimiento)
        sm = ScreenManager()
        sm.add_widget(LoginScreen(name="login"))
        sm.add_widget(MantenimientoScreen(name="mantenimiento"))
        sm.add_widget(CrearOTScreen())
        sm.add_widget(RegistrarTareaScreen(name='registrar_tarea'))
        sm.add_widget(OrdenesTrabajoScreen(name="ordenes_trabajo"))
        
        return sm

if __name__ == '__main__':
    MyApp().run()



