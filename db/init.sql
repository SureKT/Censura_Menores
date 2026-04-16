CREATE TABLE IF NOT EXISTS Solicitud (
    Id_Solicitud INT PRIMARY KEY,
    GUID_Solicitud VARCHAR(255),
    Id_Fichero VARCHAR(255),
    Inicio_Solicitud TIMESTAMP,
    Fin_Solicitud TIMESTAMP,
    Inicio_Deteccion_Caras TIMESTAMP,
    Fin_Deteccion_Caras TIMESTAMP,
    Inicio_Edad TIMESTAMP,
    Fin_edad TIMESTAMP,
    Inicio_Pixelado TIMESTAMP,
    Fin_Pixelado TIMESTAMP,
    Inicio_Almacenamiento_Solicitud TIMESTAMP,
    Fin_Almacenamiento_Solicitud TIMESTAMP,
    Num_Imagenes_Total INT,
    Num_Imagenes_Pixeladas INT,
    Estado VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS Imagenes (
    Id_Imagen INT PRIMARY KEY,
    Id_Solicitud INT,
    Estado VARCHAR(50),
    FOREIGN KEY (Id_Solicitud) REFERENCES Solicitud(Id_Solicitud)
);
