#!/usr/bin/env bash
set -euo pipefail

show_help() {
	# Usamos un "here document" para una ayuda más robusta y legible.
	cat <<-EOF
	commit.sh - Helper para automatizar commits en este repositorio.

	Uso:
	  ./commit.sh -m "mensaje"         # add ., formatea, commitea y pushea.
	  ./commit.sh -m "msg" -n          # add ., formatea, commitea (sin push).
	  ./commit.sh -m "msg" -f file1 f2 # add archivos específicos.
	  ./commit.sh -s                   # Omite los pre-checks (black/flake8).

	Opciones:
	  -m MSG     Mensaje de commit (obligatorio).
	  -n         No hacer push (solo commit local).
	  -s         Saltar pre-checks (black / flake8).
	  -f FILES   Archivos específicos a añadir (por defecto: 'git add .').
	             Debe ser el último argumento.
	  -h         Mostrar esta ayuda.

	EOF
}

MSG="Actualización"
NO_PUSH=0
SKIP_CHECKS=0
FILES=()

while [[ $# -gt 0 ]]; do
	case "$1" in
		-m)
			MSG="${2-}" # Usar ${2-} para evitar error si -m es el último arg
			shift 2
			;;
		-n)
			NO_PUSH=1
			shift
			;;
		-s)
			SKIP_CHECKS=1
			shift
			;;
		-f)
			shift
			FILES+=("$@") # Asigna todos los argumentos restantes a FILES
			break # -f debe ser la última opción
			;;
		-h|--help)
			show_help
			exit 0
			;;
		*)
			echo "Opción desconocida: $1"
			show_help
			exit 1
			;;
	esac
done

if [[ "$MSG" == "Actualización" ]]; then
	echo "Error: El mensaje de commit es obligatorio. Usa -m \"tu mensaje\"."
	show_help
	exit 1
fi

# Mostrar entorno actual (útil cuando se trabaja con conda)
if [[ -n "${CONDA_DEFAULT_ENV-}" ]]; then
	echo "Conda env activo: ${CONDA_DEFAULT_ENV}"
else
	echo "(No se detectó entorno conda activo)"
fi

# Asegurarse de que estamos en el root del repo
cd "$(git rev-parse --show-toplevel)"

echo "============================================="
echo "Mensaje de commit: $MSG"
echo "============================================="

if [[ $SKIP_CHECKS -eq 0 ]]; then
	# Formatear con black si está disponible
	if command -v black >/dev/null 2>&1; then
		echo "Running black..."
		black --quiet . || true
	fi
	# Ejecutar flake8 si está disponible
	if command -v flake8 >/dev/null 2>&1; then
		echo "Ejecutando flake8..."
		# Capturamos la salida de flake8. Si hay errores, preguntamos.
		if ! flake8; then
			read -p "flake8 encontró problemas. ¿Continuar con el commit? (s/N) " -n 1 -r
			echo # Mover a una nueva línea
			if [[ ! $REPLY =~ ^[Ss]$ ]]; then
				echo "Commit cancelado."
				exit 1
			fi
		fi
	fi
fi

if [[ ${#FILES[@]} -gt 0 ]]; then
	echo "git add ${FILES[*]}"
	git add "${FILES[@]}"
else
	echo "git add ."
	git add .
fi

# Comprobar si hay cambios para commitear antes de intentarlo
if git diff --quiet && git diff --staged --quiet; then
	echo "No hay cambios para commitear. Saliendo."
	exit 0
fi

git commit -m "$MSG"
BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ $NO_PUSH -eq 0 ]]; then
	echo "Pushing to origin/$BRANCH..."
	git push origin "$BRANCH"
else
	echo "Commit creado localmente en rama $BRANCH (no push)."
fi

echo "Hecho."
