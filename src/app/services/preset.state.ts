import { inject, Service, signal } from '@angular/core';
import { ColorModel, PaletteModel } from './color.state';
import { HttpClient } from '@angular/common/http';
import { UtilsService } from './utils.service';

export interface PresetModel {
    type: 'custom' | 'default',
    codes: string[],
    palette: PaletteModel
}

export interface PresetStateModel {
    default: PresetModel[]
    custom: PresetModel[]
}

@Service()
export class PresetState {
    http = inject(HttpClient)
    utils = inject(UtilsService)

    state = signal<PresetStateModel>({
        default: [],
        custom: [],
    })

    constructor() {
        this.http
            .get('/data/presets.json')
            .subscribe(response=>{
                const defaultPresets = response as PresetModel[]
                const customPresets = JSON.parse(localStorage.getItem('customPresets') || '[]')

                this.state.set({
                    default: defaultPresets,
                    custom: customPresets
                })
            })
    }

    checkIfExists(palette: PaletteModel):boolean {
        return !![...this.state().custom].find(el=>el.palette.name === palette.name)
    }

    savePreset(palette: PaletteModel) {
        this.state.set({
            ...this.state(),
            custom: [
                ...this.state().custom,
                {
                    type: 'custom',
                    codes: palette.colors.flatMap(el=>this.colorToCode(el)),
                    palette
                }
            ]
        })
        this._save()
    }

    updatePreset(palette: PaletteModel) {
        this.state.set({
            ...this.state(),
            custom: [
                {
                    type: 'custom',
                    codes: palette.colors.flatMap(el=>this.colorToCode(el)),
                    palette
                },
                ...this.state().custom.filter(el=>el.palette.name !== palette.name)
            ]
        })
        this._save()
    }

    deletePreset(preset: PresetModel) {
        this.state.set({
            ...this.state(),
            custom: [
                ...this.state().custom.filter(el=>el!==preset)
            ]
        })
        this._save()
    }

    colorToCode(color: ColorModel) {
        const ret = []
        for(let shade of color.shades) {
            const hsl = [color.hue, color.saturation, shade]
            const rgb = this.utils.hslToRgb(hsl[0], hsl[1], hsl[2])
            ret.push(this.utils.rgbToHex(rgb[0], rgb[1], rgb[2]))
        }
        return ret
    }

    private _save() {
        localStorage.setItem('customPresets', JSON.stringify(this.state().custom))
    }
}
